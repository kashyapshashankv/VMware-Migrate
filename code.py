import ssl
import socket
import hashlib
import time
import subprocess
import tempfile
import os
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim

def get_ssl_thumbprint(host, port=443):
    context = ssl._create_unverified_context()  # Disable cert verification
    conn = socket.create_connection((host, port))
    sock = context.wrap_socket(conn, server_hostname=host)
    der_cert = sock.getpeercert(binary_form=True)
    thumbprint = hashlib.sha1(der_cert).hexdigest().upper()
    sock.close()
    return ":".join(thumbprint[i:i+2] for i in range(0, len(thumbprint), 2))

def connect_vsphere(host, user, pwd):
    context = ssl._create_unverified_context()
    si = SmartConnect(host=host, user=user, pwd=pwd, sslContext=context)
    return si

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

def disable_cbt(vm):
    spec = vim.vm.ConfigSpec()
    spec.changeTrackingEnabled = False
    task = vm.ReconfigVM_Task(spec)
    wait_for_task(task)
    if task.info.state == 'error':
        raise Exception(f"CBT enable failed: {task.info.error.msg}")

def create_snapshot(vm, name, desc="Snapshot", memory=False, quiesce=True):
    task = vm.CreateSnapshot_Task(name=name, description=desc, memory=memory, quiesce=quiesce)
    wait_for_task(task)
    if task.info.state == 'error':
        raise Exception(f"Snapshot creation failed: {task.info.error.msg}")
    return task.info.result  # Snapshot object

def remove_all_snapshots(vm):
    """
    Recursively remove all snapshots from the VM snapshot tree.
    """
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
            # If this snapshot has children (nested snapshots), remove them as well
            if snap.childSnapshotList:
                recursive_remove(snap.childSnapshotList)

    recursive_remove(vm.snapshot.rootSnapshotList)
    print("All snapshots removed.")

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

def get_cbt_changed_blocks(vm, start_snap_obj, curr_snap_obj, disk_key):
    changed_areas = []
    try:
        changes = vm.QueryChangedDiskAreas(
            startSnapshot=start_snap_obj,
            endSnapshot=curr_snap_obj,
            diskKey=disk_key
        )
        sectorsize = 512
        for area in changes:
            for change in area.changeArea:
                offset_bytes = change.offset * sectorsize
                length_bytes = change.length * sectorsize
                changed_areas.append((offset_bytes, length_bytes))
    except Exception as e:
        print(f"Error querying changed disk areas: {e}")
    return changed_areas

def start_nbdkit(vcenter, user, pwd, thumbprint, vm_moref, vmdk_path, snapshot_moref):
    cmd = [
        "nbdkit", "vddk","libdir=/root/vmware-vix-disklib-distrib",
        "--verbose",
        "--exit-with-parent",
        "-D vddk.datapath=0",
		"-D nbdkit.backend.datapath=0",
        f"user={user}",
        f"password={pwd}",
        f"server={vcenter}",
        f"thumbprint={thumbprint}",        
        "--foreground",
        "--readonly",
		"compression=fastlz",
        "transports=nbd",
        "config=/root/vddk.conf",
        f"vm=moref={vm_moref}",
        f"snapshot={snapshot_moref}",
        vmdk_path,
    ]
    print(cmd)
    proc = subprocess.Popen(cmd)
    time.sleep(5)
    return proc

def terminate_nbdkit(proc):
    proc.terminate()
    proc.wait()

def full_copy(vmdk_nbd_url, output_file):
    print(f"Starting full copy using nbdcopy to {output_file} ...")
    subprocess.run(["nbdcopy", "--progress" ,vmdk_nbd_url, output_file], check=True)
    print("Full disk copy completed.")

def delta_copy(vmdk_nbd_url, changed_blocks, output_file):
    print("Starting delta copy using nbdsh...")

    # Prepare blocks list Python literal string
    blocks_str = "[" + ",".join(f"({offset},{length})" for offset, length in changed_blocks) + "]"

    nbdsh_script = f"""
import sys
blocks = {blocks_str}

with open(sys.argv[1], "wb") as out:
    for offset, length in blocks:
        data = h.pread(length, offset)
        out.write(offset.to_bytes(8, 'big'))
        out.write(length.to_bytes(8, 'big'))
        out.write(data)
"""

    with tempfile.NamedTemporaryFile("w", delete=False) as script_file:
        script_file.write(nbdsh_script)
        script_path = script_file.name

    try:
        subprocess.run(["nbdsh", "-u", vmdk_nbd_url, script_path, output_file], check=True)
        print(f"Delta copy to {output_file} completed.")
    finally:
        os.remove(script_path)

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

            full_data[offset:offset+length] = data

    with open(merged_output_path, "wb") as f_out:
        f_out.write(full_data)
    print(f"Merged disk image written to {merged_output_path}")

def main():
    vcenter = "vcsa.example.local"
    user = "Administrator@vsphere.local"
    pwd = "Test"
    vm_name = "VM_MIGRATION_TEST"

    si = connect_vsphere(vcenter, user, pwd)
    vm = find_vm(si, vm_name)
    if not vm:
        raise RuntimeError(f"VM '{vm_name}' not found")
    vm_moref = vm._moId

    thumbprint = get_ssl_thumbprint(vcenter)

    # print("Enabling CBT...")
    enable_cbt(vm)
    # disable_cbt(vm)
    disks = [dev for dev in vm.config.hardware.device if isinstance(dev, vim.vm.device.VirtualDisk)]
    if not disks:
        raise RuntimeError("No VM virtual disks found")

    print(f"Found {len(disks)} disks. Starting processing for each disk...")
    print(f"Deleting All Snapshots from VM")
    remove_all_snapshots(vm)
    print("Creating first snapshot (powered on, quiesced)...")
    snap1 = create_snapshot(vm, "full_copy_snapshot", quiesce=True)

    merged_images = []
    print()
    for idx, disk in enumerate(disks):
        # print(disk.backing.fileName)
        print(f"Processing disk {idx}: {disk.backing.parent.fileName}")

        vmdk_path = disk.backing.parent.fileName
        output_full = f"disk{idx}_full_copy.raw"
        merged_output = f"disk{idx}_merged_disk_image.raw"
        vmdk_nbd_url = "nbd://localhost"  # access local nbdkit server

        # Start nbdkit serving this disk snapshot
        print(f"Starting nbdkit for full copy disk {idx}...")
        nbdkit_proc = start_nbdkit(vcenter, user, pwd, thumbprint, vm_moref, vmdk_path, snap1._moId)

        # Full copy using nbdcopy CLI
        full_copy(vmdk_nbd_url, output_full)

        terminate_nbdkit(nbdkit_proc)

        merged_images.append((disk, output_full, merged_output))

    print("Removing first snapshot...")
    remove_snapshot(snap1)

    print("Powering off VM...")
    poweroff_vm(vm)

    print("Creating second snapshot (powered off)...")
    snap2 = create_snapshot(vm, "delta_copy_snapshot", quiesce=False)

    for idx, (disk, output_full, merged_output) in enumerate(merged_images):
        print(f"Processing delta copy for disk {idx}: {disk.backing.parent.fileName}")

        vmdk_path = disk.backing.parent.fileName
        disk_key = disk.key
        vmdk_nbd_url = "nbd://localhost"

        changed_blocks = get_cbt_changed_blocks(vm, snap1, snap2, disk_key)
        print(f"Disk {idx} changed blocks: {len(changed_blocks)}")

        # Start nbdkit for delta copy snapshot
        nbdkit_proc = start_nbdkit(vcenter, user, pwd, thumbprint, vm_moref, vmdk_path, snap2._moId)

        delta_output = f"disk{idx}_delta_copy.dat"
        delta_copy(vmdk_nbd_url, changed_blocks, delta_output)

        terminate_nbdkit(nbdkit_proc)

        print(f"Applying delta to full copy for disk {idx}...")
        apply_delta_to_full(output_full, delta_output, merged_output)

        # Clean intermediate files
        os.remove(output_full)
        os.remove(delta_output)

        print(f"Disk {idx} merged image created at {merged_output}")

    print("Removing second snapshot...")
    remove_snapshot(snap2)

    Disconnect(si)
    print("All disks processed. Workflow completed successfully.")

if __name__ == "__main__":
    main()
