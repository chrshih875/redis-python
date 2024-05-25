from time import time

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
        if self.ID == "*":
            self.ID = self.time_number_auto_generate()
            return [2]

        curr_ID_time, curr_ID_sequence = self.ID.split("-")
        if curr_ID_sequence == "*":
            curr_ID_sequence = self.sequence_number_auto_generate(curr_ID_time, curr_ID_sequence)
            self.ID = f"{curr_ID_time}-{curr_ID_sequence}"
            return [2]
        
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

    def sequence_number_auto_generate(self, curr_ID_time, curr_ID_sequence):
        if self.key not in self.log:
            curr_ID_sequence = 1 if int(curr_ID_time) == 0 else 0
        else:
            last_ID_time, last_ID_sequence = self.log[self.key][-1][0].split("-")
            curr_ID_sequence = int(last_ID_sequence) + 1 if int(curr_ID_time) == 0 or curr_ID_time ==  last_ID_time else 0
        return str(curr_ID_sequence)

    def time_number_auto_generate(self):
        curr_ID_time = int(time() * 1000)
        if self.key not in self.log:
            return f"{curr_ID_time}-{0}"
        else:
            last_ID_time, last_ID_sequence = self.log[self.key][-1][0].split("-")
            curr_ID_sequence = last_ID_sequence + 1 if curr_ID_time == int(last_ID_time) else 0
        return f"{curr_ID_time}-{curr_ID_sequence}"

    def return_ID(self):
        return f"${len(self.ID)}\r\n{self.ID}\r\n".encode()
    
    def finding_xRange(self, args):
        array = []
        self.value = args[1:]
        start_time, start_sequence, end_time, end_sequence = self.value[0][0], self.value[0][-1], self.value[1][0], self.value[1][-1]
        if args[-1] == "+":
            end_time, end_sequence = self.log[self.key][-1][0].split("-")
        if args[-1] == "-":
            start_time, start_sequence = self.log[self.key][0][0].split("-")
        curr_val = self.log[self.key]
        for val in curr_val:
            val_time, val_sequence = val[0][0], val[0][-1]
            if start_time <= val_time <= end_time and start_sequence <= val_sequence <= end_sequence:
                array.append(val)
        signal_array = []
        signal_array.append(f"*{len(array)}\r\n")
        for val in array:
            split_val = val[-1].split(" ")
            signal_array.append(f"*{len(val)}\r\n${len(val[0])}\r\n{val[0]}\r\n*{len(split_val)}\r\n")
            for i in range(len(split_val)):
                signal_array.append(f"${len(split_val[i])}\r\n{split_val[i]}\r\n")
        return "".join(signal_array).encode()
    