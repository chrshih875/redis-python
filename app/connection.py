from threading import *
from time import time
import os

class Connection(Thread):
    def __init__(self, socket, address, dir, dbfilename):
        super().__init__()
        self.socket = socket
        self.address = address
        self.dir = dir
        self.filename = dbfilename
        self.store = {}
        self.expiry_time = {}
        self.config_get = {}
        self.start()

    def run(self):
        while True:
            request = self.socket.recv(4096)
            if request:
                command = self.parse_request(request)
                print("PARSE_REQ", command) 
                self.parse_command(command)

    def parse_request(self, request):
        return request.decode().split("\r\n")[2:-1:2]

    def parse_command(self, command):
        # print("COMMAND",command)
        check_command = command[0].upper()
        match check_command:
            case "PING":
                self.socket.send("+PONG\r\n".encode())
            case "ECHO":
                self.socket.send(f"+{command[1]}\r\n".encode())
            case "SET":
                self.store[command[1]] = command[2]
                if len(command) > 3 and command[3].lower() == "px":
                    additional_time = int(command[-1])
                    self.expiry_time[command[1]] = additional_time+(time()*1000)
                self.socket.send("+OK\r\n".encode())
            case "GET":
                if self.dir and self.filename:
                    rdb_key, rdb_value, key_length, value_length = self.read_rdb_file(self.dir, self.filename)
                    self.socket.send(f"${value_length}\r\n{rdb_value}\r\n".encode())
                    # print("returned data", rdb_key, rdb_value, key_length, value_length) 
                elif command[1] in self.expiry_time and self.expiry_time[command[1]] >= time() * 1000 or command[1] not in self.expiry_time:
                    self.socket.send(f"+{self.store[command[1]]}\r\n".encode())
                else:
                    self.socket.send("$-1\r\n".encode())
            case "CONFIG" | "GET":
                self.socket.send(f"*2\r\n$3\r\n{command[-1]}\r\n${len(self.dir)}\r\n{self.dir}\r\n".encode())
            case "KEYS":
                rdb_key, rdb_value, key_length, value_length = self.read_rdb_file(self.dir, self.filename)
                self.socket.send(f"*1\r\n${key_length}\r\n{rdb_key}\r\n".encode())
        print("Sent message")

    def finding_rdb_key_length(self, file):
        while op := file.read(1):
            if op == b"\xfb":
                break
        data = file.read(4)
        return data[-1]
    
    def finding_rdb_value_length(self, file):
        length = file.read(1)
        return length[-1]
    
    def read_rdb_file(self, dir, dbfilename):
        file_path = os.path.join(dir, dbfilename)
        if not os.path.exists(file_path):
            return
        with open(file_path, 'rb') as file:
            key_length = self.finding_rdb_key_length(file)
            key = file.read(key_length).decode('utf-8')
            value_length = self.finding_rdb_value_length(file)
            value = file.read(value_length).decode('utf-8')
            return key, value, key_length, value_length
