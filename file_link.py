from datetime import datetime
import json
import yaml
import copy

class FileLink:
    def __init__(self, file_path, file_format):
        self.file_path = file_path
        self.file_format = file_format
        
        try:
            self.fd = open(self.file_path, 'w')
        except Exception as e:
            print('Error occurred: ', e)

    def write(self, info):
        if self.file_format == 'json':
            json.dump(info, self.fd)

        elif self.file_format == 'yaml':
            self.fd.write('---\n')
            yaml.dump(info, self.fd)
