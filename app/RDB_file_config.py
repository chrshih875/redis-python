import os
from time import time

class RDB_fileconfig:
    def __init__(self, dir, dbfilename):
        self.dir = dir
        self.filename = dbfilename

    def finding_rdb_key_start_point(self, file):
        total_keys = []
        while op := file.read(1):
            if op == b"\xfb":
                break
        file.read(2)

        dictionary = {}
        while op := file.read(1):
            if op == b'\xfc':
                while op == b'\xfc':
                    expiry = int.from_bytes(file.read(8), byteorder="little")
                    file.read(2)
                    op, total_keys = self.finding_key_value(file, total_keys)
                    dict1 = self.key_value_pair_expire(dictionary, total_keys, expiry)
                    total_keys = []
                return dict1
            else:
                if op == b"\x00":
                    op = file.read(1)
                    while op != b"\xff":
                        if op == b"\x00":
                            op = file.read(1)
                        total_keys.append(op)
                        op = file.read(1)
            return self.key_value_pair(total_keys)
        
    def key_value_pair_expire(self, dictionary, total_keys, expiry):
        key, val = "", ""
        switch = True
        for byte in total_keys:
            byte = byte.decode('utf-8')
            if not switch:
                val+=byte
            elif byte.isalpha() and switch:
                key+=byte
            else:
                switch = False
        dictionary[key] = {"value": val, "expiry": expiry/1000}
        return dictionary
    
    def finding_key_value(self, file, total_keys):
        op = file.read(1)
        while op != b"\xfc" and op != b"\xff":
            total_keys.append(op)
            op = file.read(1)
        return op, total_keys

    def key_value_pair(self, total_keys):
        strings = ""
        dictionary = {}
        switch = [True, "tmp"]
        for idx, byte in enumerate(total_keys):
            if idx == 0:
                continue
            byte = byte.decode('utf-8')
            if byte.isalpha():
                strings+=byte
            elif switch[0]:
                dictionary[strings] = "tmp"
                switch[0], switch[1], strings = False, strings, ""
            else:
                dictionary[switch[1]] = strings
                switch[0], strings = True, ""
        dictionary[switch[1]] = strings
        return dictionary

    def return_value_only(self, dictionary):
        revised = {}
        for key, val in dictionary.items():
            if "expiry" in val and val["expiry"] >= time():
                revised[key] = f"${len(val["value"])}\r\n{val["value"]}\r\n".encode()
            elif "expiry" in val and val["expiry"] <= time():
                revised[key] = "$-1\r\n".encode()
            else:
                revised[key] = f"${len(val)}\r\n{val}\r\n".encode()
        return revised

    def return_key_only(self, dictionary):
        keys = dictionary.keys()
        array = [f"*{len(keys)}\r\n"]
        for key in keys:
            array.append(f"${len(key)}\r\n{key}\r\n")
        return "".join(array).encode()
        
    def read_rdb_file(self, dir, dbfilename, command):
        file_path = os.path.join(dir, dbfilename)
        if not os.path.exists(file_path):
            return
        with open(file_path, 'rb') as file:
            dictionary = self.finding_rdb_key_start_point(file)
            match command:
                case "GET":
                    return self.return_value_only(dictionary)
                case "KEYS":
                    return self.return_key_only(dictionary)
                