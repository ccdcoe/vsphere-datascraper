class DuplicateDetection:
    def __init__(self, mode):
        self.ip_duplicates = dict()
        self.hostname_duplicates = dict()
        self.dupl_mode = mode
    
    def find_duplicates(self, info):
        if self.dupl_mode == "hostname" or self.dupl_mode == "all":
            hostname = info.get("host_name", "Unknown")
            if hostname in self.hostname_duplicates:
                infolist = self.hostname_duplicates[hostname]
                infolist.append(info)
                self.hostname_duplicates[hostname] = infolist
            else:
                self.hostname_duplicates[hostname] = [ info ]
    
        elif self.dupl_mode == "ip" or self.dupl_mode == "all":
            for nicinfo in info.get("nic", []):
                for ip in nicinfo.get("IP", "Unknown"):
                    if ip in self.ip_duplicates:
                        infolist = self.ip_duplicates[ip]
                        infolist.append(info)
                        self.hostname_duplicates[ip] = infolist
                    else:
                        self.hostname_duplicates[ip] = [ info ]

    def print_duplicates(self):
        to_be_checked = list()
        if self.dupl_mode == "hostname" or self.dupl_mode == "all":
            to_be_checked.append(self.hostname_duplicates)
        elif self.dupl_mode == "ip" or self.dupl_mode == "all":
            to_be_checked.append(self.ip_duplicates)

        for dupldict in to_be_checked:
            for key, value in dupldict.items():
                if len(value) > 1:
                    print("Found duplicates for " + str(key))
                    #print(value)
