import ssl
import socket
import hashlib
import time
import subprocess
import tempfile
import os
import threading
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import nbd
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_ssl_thumbprint(host, port=443):
    context = ssl._create_unverified_context()  # Disable cert verification for simplicity
    conn = socket.create_connection((host, port))
    sock = context.wrap_socket(conn, server_hostname=host)
    der_cert = sock.getpeercert(binary_form=True)
    thumbprint = hashlib.sha1(der_cert).hexdigest().upper()
    sock.close()
    return ":".join(thumbprint[i:i + 2] for i in range(0, len(thumbprint), 2))


def connect_vsphere(host, user, pwd):
    context = ssl._create_unverified_context()
    si = SmartConnect(host=host, user=user, pwd=pwd, sslContext=context)
    return si


def remove_all_snapshots(vm):
    if not vm.snapshot:
        print("No snapshots found to remove.")
        return

    def recursive_remove(snapshot_tree):
        for snap in snapshot_tree:
            print(f"Removing snapshot: {snap.name}")
            task = snap.snapshot.RemoveSnapshot_Task(removeChildren=False)
            wait_for_task(task)
            if task.info.state == 'error':
                raise Exception(f"Failed to remove snapshot '{snap.name}': {task.info.error.msg}")
            if snap.childSnapshotList:
                recursive_remove(snap.childSnapshotList)

    recursive_remove(vm.snapshot.rootSnapshotList)
    print("All snapshots removed.")


def find_vm(si, vm_name):
    content = si.RetrieveContent()
    container = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
    for vm in container.view:
        if vm.name == vm_name:
            return vm
    return None


def enable_cbt(vm):
    spec = vim.vm.ConfigSpec()
    spec.changeTrackingEnabled = True
    task = vm.ReconfigVM_Task(spec)
    wait_for_task(task)
    if task.info.state == 'error':
        raise Exception(f"CBT enable failed: {task.info.error.msg}")


def create_snapshot(vm, name, desc="Snapshot", memory=False, quiesce=True):
    task = vm.CreateSnapshot_Task(name=name, description=desc, memory=memory, quiesce=quiesce)
    wait_for_task(task)
    if task.info.state == 'error':
        raise Exception(f"Snapshot creation failed: {task.info.error.msg}")
    return task.info.result


def remove_snapshot(snapshot, remove_children=False):
    task = snapshot.RemoveSnapshot_Task(removeChildren=remove_children)
    wait_for_task(task)
    if task.info.state == 'error':
        raise Exception(f"Snapshot removal failed: {task.info.error.msg}")


def poweroff_vm(vm):
    if vm.runtime.powerState != vim.VirtualMachinePowerState.poweredOff:
        task = vm.PowerOffVM_Task()
        wait_for_task(task)
        if task.info.state == 'error':
            raise Exception(f"Failed to power off VM: {task.info.error.msg}")


def wait_for_task(task):
    while task.info.state not in [vim.TaskInfo.State.success, vim.TaskInfo.State.error]:
        time.sleep(1)


def get_cbt_changed_blocks(vm, target_disk_key, changeId):
    changed_areas = []
    try:
        print(f"Querying changed disk areas with changeId={changeId} for disk key={target_disk_key}")
        changes = vm.QueryChangedDiskAreas(deviceKey=target_disk_key, startOffset=0, changeId=changeId)
        sectorsize = 512
        for change in changes.changedArea:
            offset_bytes = change.offset * sectorsize
            length_bytes = change.length * sectorsize
            changed_areas.append((offset_bytes, length_bytes))
        print(f"Found {len(changed_areas)} changed areas")
    except Exception as e:
        print(f"Error querying changed disk areas: {e}")
    return changed_areas


def start_nbdkit(vcenter, user, pwd, thumbprint, vm_moref, vmdk_path, snapshot_moref):
    cmd = [
        "nbdkit", "vddk", "libdir=/root/python_migration/vmware-vix-disklib-distrib",
        # "--verbose",
        "--exit-with-parent",
        f"user={user}",
        f"password={pwd}",
        f"server={vcenter}",
        f"thumbprint={thumbprint}",
        "--foreground",
        "--readonly",
        "compression=fastlz",
        "transports=nbd",
        "config=/root/python_migration/vddk.conf",
        f"vm=moref={vm_moref}",
        f"snapshot=moref={snapshot_moref}",
        vmdk_path,
    ]
    proc = subprocess.Popen(cmd)
    time.sleep(5)  # Allow nbdkit to initialize
    return proc


def terminate_nbdkit(proc):
    proc.terminate()
    proc.wait()


def open_nbd_connection(server_url="nbd://localhost"):
    nbd_ctx = nbd.NBD()
    nbd_ctx.connect_uri(server_url)
    return nbd_ctx


def full_copy_with_nbdcopy(vmdk_nbd_url, output_file, total_size):
    print("Starting full copy with nbdcopy and progress display...")
    proc = subprocess.Popen(
        ["nbdcopy", "--progress", vmdk_nbd_url, output_file],
        stderr=subprocess.PIPE,
        universal_newlines=True
    )

    for line in proc.stderr:
        if '%' in line:
            print(f"\rFull copy progress: {line.strip()}", end='', flush=True)

    proc.wait()
    print("\nFull disk copy completed.")


def read_chunk_with_retry(nbd_ctx, offset, length, retries=3, delay=2):
    for attempt in range(1, retries + 1):
        try:
            data = nbd_ctx.pread(length, offset)
            return offset, data
        except Exception as e:
            print(f"Read chunk at offset {offset} failed (attempt {attempt}): {e}")
            if attempt == retries:
                raise
            time.sleep(delay)


def read_blocks(nbd_ctx, blocks):
    block_data = {}
    total_blocks = len(blocks)
    for idx, (offset, length) in enumerate(blocks, start=1):
        data = nbd_ctx.pread(length, offset)
        block_data[offset] = data
        print(f"\rReading changed blocks: {idx}/{total_blocks}", end='', flush=True)
    print()
    return block_data


def write_delta_file(blocks_data, output_path):
    with open(output_path, "wb") as f:
        for offset in sorted(blocks_data.keys()):
            data = blocks_data[offset]
            f.write(offset.to_bytes(8, 'big'))
            f.write(len(data).to_bytes(8, 'big'))
            f.write(data)
    print(f"Delta output written to {output_path}")


def apply_delta_to_full(full_path, delta_path, merged_output_path):
    with open(full_path, "rb") as f_full:
        full_data = bytearray(f_full.read())

    with open(delta_path, "rb") as f_delta:
        while True:
            offset_bytes = f_delta.read(8)
            if not offset_bytes or len(offset_bytes) < 8:
                break
            length_bytes = f_delta.read(8)
            if len(length_bytes) < 8:
                raise ValueError("Delta file corrupt or incomplete")
            offset = int.from_bytes(offset_bytes, 'big')
            length = int.from_bytes(length_bytes, 'big')
            data = f_delta.read(length)
            if len(data) < length:
                raise ValueError("Delta file corrupt or incomplete")

            full_data[offset:offset + length] = data

    with open(merged_output_path, "wb") as f_out:
        f_out.write(full_data)
    print(f"Merged disk image written to {merged_output_path}")


def get_snapshot_disk_path(snapshot, original_disk):
    # Traverse snapshot config hardware to find disk backing for original disk key
    snap_config = getattr(snapshot, 'config', None)
    if snap_config:
        for dev in snap_config.hardware.device:
            if isinstance(dev, vim.vm.device.VirtualDisk) and dev.key == original_disk.key:
                return dev
    # Fallback to original disk path if snapshot config unavailable
    return original_disk




def main():
    vcenter = "vcsa.cloudbricks.local"
    user = "Administrator@vsphere.local"
    pwd = "VMware1!"
    vm_name = "VM_MIGRATION_TEST"

    try:
        si = connect_vsphere(vcenter, user, pwd)
        vm = find_vm(si, vm_name)
        if not vm:
            raise RuntimeError(f"VM '{vm_name}' not found")
        vm_moref = vm._moId

        thumbprint = get_ssl_thumbprint(vcenter)

        print("Enabling CBT...")
        enable_cbt(vm)

        remove_all_snapshots(vm)

        print("Creating first snapshot (powered on, quiesced)...")
        snap1 = create_snapshot(vm, "full_copy_snapshot", quiesce=True)

        merged_images = []
        vm = find_vm(si, vm_name)
        disks = [dev for dev in vm.config.hardware.device if isinstance(dev, vim.vm.device.VirtualDisk)]
        if not disks:
            raise RuntimeError("No VM virtual disks found")

        print(f"Found {len(disks)} disks. Starting processing for each disk...")

        for idx, disk in enumerate(disks):
            

            # Use snapshot disk backing path for live consistent copy
            snapshot = get_snapshot_disk_path(snap1, disk)
            vmdk_path = snapshot.backing.fileName
            print(f"Processing disk {idx}: {vmdk_path}")
            output_full = f"{vm_name}_disk{idx}_full_copy.raw"
            output_delta = f"{vm_name}_disk{idx}_delta_copy.dat"
            merged_output = f"{vm_name}_disk{idx}_merged_disk_image.raw"

            print(f"Starting nbdkit for full copy disk {idx}...")
            nbdkit_proc = start_nbdkit(vcenter, user, pwd, thumbprint, vm_moref, vmdk_path, snap1._moId)

            vmdk_nbd_url = "nbd://localhost"
            full_copy_with_nbdcopy(vmdk_nbd_url, output_full, disk.capacityInBytes)

            terminate_nbdkit(nbdkit_proc)

            merged_images.append((output_full, output_delta, merged_output))

        print("Removing first snapshot...")
        remove_snapshot(snap1)
        time.sleep(100)
        print("Powering off VM...")
        poweroff_vm(vm)

        print("Creating second snapshot (powered off)...")
        snap2 = create_snapshot(vm, "delta_copy_snapshot", quiesce=False)
        vm = find_vm(si, vm_name)
        disks = [dev for dev in vm.config.hardware.device if isinstance(dev, vim.vm.device.VirtualDisk)]
        if not disks:
            raise RuntimeError("No VM virtual disks found")

        for idx, disk in enumerate(disks):
            # print(f"Processing delta copy for disk {idx}: {disk.backing.fileName}")
            snapshot = get_snapshot_disk_path(snap2, disk)
            vmdk_path = snapshot.backing.fileName
            disk_key = snapshot.key
            print(f"Processing delta copy for disk {idx}: {vmdk_path}")
            changeId = getattr(disk.backing, 'changeId', None)
            print(f"Disk {idx} changeId: {changeId}")
            if changeId is None:
                print(f"No changeId found for disk {idx}. Skipping delta copy.")
                continue

            output_full, output_delta, merged_output = merged_images[idx]

            changed_blocks = get_cbt_changed_blocks(vm, disk_key, changeId)
            print(f"Disk {idx} changed blocks count: {len(changed_blocks)}")
            if len(changed_blocks) > 0:
                print(f"Changed blocks detail: {changed_blocks}")
                print(f"Starting nbdkit for delta copy disk {idx}...")
                nbdkit_proc = start_nbdkit(vcenter, user, pwd, thumbprint, vm_moref, vmdk_path, snap2._moId)
                nbd_ctx = open_nbd_connection()

                blocks_data = read_blocks(nbd_ctx, changed_blocks)
                write_delta_file(blocks_data, output_delta)

                terminate_nbdkit(nbdkit_proc)
                nbd_ctx.close()

                print(f"Applying delta to full copy for disk {idx}...")
                apply_delta_to_full(output_full, output_delta, merged_output)

                os.remove(output_full)
                os.remove(output_delta)
            else:
                print("No Delta Changes Found. Generating Final File")
                os.rename(output_full, merged_output)

            print(f"Disk {idx} merged image created at {merged_output}")

        print("Removing second snapshot...")
        remove_snapshot(snap2)

    except Exception as e:
        print(f"Error occurred: {e}")
        if 'nbdkit_proc' in locals():
            terminate_nbdkit(nbdkit_proc)
        raise
    finally:
        if 'si' in locals():
            Disconnect(si)
        print("Disconnected from vCenter.")

    print("All disks processed. Workflow completed successfully.")


if __name__ == "__main__":
    main()
