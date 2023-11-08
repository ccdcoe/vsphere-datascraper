import atexit
from pyVmomi import vim
from pyVim.connect import SmartConnect, Disconnect
from datetime import datetime

class VMvCenter:
    def __init__(self,
                 host,
                 user,
                 password,
                 port,
                 disable_ssl_verification,
                 folder_path):
        # Init vars
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.disable_ssl_verification = disable_ssl_verification
        self.folder_path = folder_path

    def connect(self):
        if self.disable_ssl_verification:
            self.si = SmartConnect(host=self.host,
                                   user=self.user,
                                   pwd=self.password,
                                   port=self.port,
                                   disableSslCertValidation=True)
        else:
            self.si = SmartConnect(host=self.host,
                                   user=self.user,
                                   pwd=self.password,
                                   port=self.port)
        # Doing this means you don't need to remember to disconnect your script/objects
        atexit.register(Disconnect, self.si)


    def get_vm_iterator_from_folder(self):
        content = self.si.RetrieveContent()
        
        # NB! Currently selects the first "Datacenter" as the root folder. If you have more visible for your user, then this needs to be expanded
        datacenter = content.rootFolder.childEntity[0]
        
        # Navigate to the starting base folder
        container = VMvCenter.get_vm_folder(self.folder_path, datacenter.vmFolder)

        container_view = content.viewManager.CreateContainerView(
            container,
            [vim.VirtualMachine], # object types to look for
            True) # whether we should look into it recursively

        return container_view.view

    def get_vm_folder(path, vm_folder):
        path = path.split('/')

        for p in path:
            vm_folder = VMvCenter.get_child(p, vm_folder.childEntity)
        return vm_folder

    def get_child(name, child_entity):
        for child in child_entity:
            if name == child.name:
                return child
        return None

    def get_vm_info(self, virtual_machine):
        vm_info = {
            'ts': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z',
            'host_name': virtual_machine.guest.hostName,
            'guest_state': virtual_machine.guest.guestState,
            'os': virtual_machine.guest.guestFullName,
            'name': virtual_machine.summary.config.name,
            'instance_uuid': virtual_machine.summary.config.instanceUuid,
            'nic': []
        }

        # Get network info
        for nic in virtual_machine.guest.net:
            if nic.ipConfig is not None:
                nic_info = {
                    'connected': nic.connected,
                    'mac': nic.macAddress,
                    'IP': [''.join([adr.ipAddress, '/', str(adr.prefixLength)]) for adr in nic.ipConfig.ipAddress],
                }
                vm_info['nic'].append(nic_info)

        return vm_info

    #def get_all_vm_info(self):
    #    vms = self.get_vm_iterator_from_folder(self.args.path)
    #    for vm in vms:
    #        yield self.get_vm_info(vm)
