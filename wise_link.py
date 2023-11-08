import json

class WiseLink:
    def __init__(self, file_path, wise_full_mode):
        self.file_path = file_path
        self.wise_full_mode = wise_full_mode
        self.wise_data = list()
        self.ip_duplicates = dict()
        try:
            self.fd = open(self.file_path, 'w')
        except Exception as e:
            print('Error occurred: ', e)


    def append(self, info):
        # Wise formatting
        for nicinfo in info.get("nic", []):
            template = {}
            template["asset"] = info.get("name", "Unknown")
            if self.wise_full_mode:
                template["os"] = info.get("os") if info.get("os") != None else "Unknown"
                template["hostname"] = info.get("host_name") if info.get("host_name") != None else "Unknown"
                template["mac"] = nicinfo.get("mac", "Unknown")
            for ip in nicinfo.get("IP", "Unknown"):
                if len(ip) > 0 and ip is not None:
                    template["ip"] = ip
                    self.wise_data.append(template)

    def write(self):
        json.dump(self.wise_data, self.fd)
