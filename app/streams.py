from collections import defaultdict

class Streams:
    def __init__(self, args, stream_logs):
        self.key = args[0]
        self.ID = args[1]
        self.value = " ".join(args[2:])
        self.log = stream_logs

    def add_log(self):
        valid = self.validate_entry_IDs()
        match valid:
            case [1]:
                return  "-ERR The ID specified in XADD must be greater than 0-0\r\n".encode()
            case [2]:
                self.log[self.key].append((self.ID, self.value))
                return self.return_ID()
            case [3]:
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".encode()
    
    def validate_entry_IDs(self):
        curr_ID_time, curr_ID_sequence = self.ID.split("-")
        print("curr id", curr_ID_time, curr_ID_sequence)
        if int(curr_ID_time) == 0 and int(curr_ID_sequence) == 0:
            return [1]
        
        if self.key not in self.log:
            return [2]
        
        last_ID_time, last_ID_sequence = self.log[self.key][-1][0].split("-")
        if (int(curr_ID_time) > int(last_ID_time) or 
        int(curr_ID_time) == int(last_ID_time) and 
        int(curr_ID_sequence) > int(last_ID_sequence)):
            return [2]
        return [3]

    def return_ID(self):
        return f"${len(self.ID)}\r\n{self.ID}\r\n".encode()
        
