from threading import Thread
from time import time
import os
from app.RDB_file_config import RDB_fileconfig

class Connection(Thread, RDB_fileconfig):
    def __init__(self, socket, address, dir, dbfilename):
        super().__init__()
        self.socket = socket
        self.address = address
        self.path = RDB_fileconfig(dir, dbfilename)
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
                if self.path.dir and self.path.filename:
                    signal = self.read_rdb_file(self.path.dir, self.path.filename, "GET")
                    self.socket.send(signal[command[1]])
                elif command[1] in self.expiry_time and self.expiry_time[command[1]] >= time() * 1000 or command[1] not in self.expiry_time:
                    self.socket.send(f"+{self.store[command[1]]}\r\n".encode())
                else:
                    self.socket.send("$-1\r\n".encode())
            case "CONFIG" | "GET":
                self.socket.send(f"*2\r\n$3\r\n{command[-1]}\r\n${len(self.path.dir)}\r\n{self.path.dir}\r\n".encode())
            case "KEYS":
                signal = self.read_rdb_file(self.path.dir, self.path.filename, "KEYS")
                self.socket.send(signal)
            case "TYPE":
                if command[1] in self.store and isinstance(self.store[command[1]], str):
                    self.socket.send("+string\r\n".encode())
                else:
                    self.socket.send("+none\r\n".encode())
        print("Sent message")
