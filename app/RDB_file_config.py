
import os

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

            while op := file.read(1):
                if op == b"\x00":
                    op = file.read(1)
                    while op != b"\xff":
                        if op == b"\x00":
                            op = file.read(1)
                        total_keys.append(op)
                        op = file.read(1)
            return self.key_value_pair(total_keys)
        
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
                print("hit2", strings)
                dictionary[switch[1]] = strings
                switch[0], strings = True, ""
        dictionary[switch[1]] = strings
        return dictionary

    def return_value_only(self, dictionary):
        values = dictionary.values()
        array = []
        for value in values:
            array.append(f"${len(value)}\r\n{value}\r\n")
        print("values", array)
        return "".join(array).encode()

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
            match command:
                case "GET":
                    dictionary = self.finding_rdb_key_start_point(file)
                    return self.return_value_only(dictionary)
                case "KEYS":
                    dictionary = self.finding_rdb_key_start_point(file)
                    return self.return_key_only(dictionary)