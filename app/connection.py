from threading import *
from time import time

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
                parse_request = self.parse_request(request)
                print("PARSE_REQ", parse_request) 
                self.parse_command(parse_request)

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
                response = self.store[command[1]]
                if response:
                    if command[1] in self.expiry_time and self.expiry_time[command[1]] >= time() * 1000 or command[1] not in self.expiry_time:
                        self.socket.send(f"+{response}\r\n".encode())
                    else:
                        self.socket.send("$-1\r\n".encode())
                else:
                    self.socket.send("$-1\r\n".encode())
            case "CONFIG":
                self.socket.send(f"*2\r\n$3\r\n{command[-1]}\r\n${len(self.dir)}\r\n{self.dir}\r\n".encode())
        print("Sent message")
