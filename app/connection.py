from threading import Thread
from time import time
from app.RDB_file_config import RDB_fileconfig
from app.streams import Streams

class Commands(Thread, RDB_fileconfig, Streams):
    def __init__(self, socket, address, dir, dbfilename, share_data):
        super().__init__()
        self.socket = socket
        self.address = address
        self.path = RDB_fileconfig(dir, dbfilename)
        self.store = {}
        self.expiry_time = {}
        self.config_get = {}
        self.share_data = share_data
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
                if command[1] in self.share_data.stream_log:
                    self.socket.send("+stream\r\n".encode())
                elif command[1] in self.store and isinstance(self.store[command[1]], str):
                    self.socket.send("+string\r\n".encode())
                else:
                    self.socket.send("+none\r\n".encode())
            case "XADD":
                stream = Streams(command[1:], self.share_data.stream_log)
                with self.share_data.condition:
                    if self.share_data.block_reads and self.share_data.block_reads[0] <= time()+1:
                        signal = stream.add_log(command[1], command[2], " ".join(command[3:]))
                        self.share_data.condition.notify_all()
                        self.socket.send(signal)
                    else:
                        signal = stream.add_log(command[1], command[2], " ".join(command[3:]))
                        self.share_data.condition.notify_all()
                        self.socket.send(signal)
            case "XRANGE":
                stream = Streams(command[1:], self.share_data.stream_log)
                signal = stream.finding_xRange(command[1:])
                self.socket.send(signal)
            case "XREAD":
                stream = Streams(command[1:], self.share_data.stream_log)
                if command[1] == "block":
                    if command[2] == "0":
                        with self.share_data.condition:
                            if self.share_data.condition.wait():
                                data = self.share_data.stream_log[command[4]][-1]
                                signal = self.block_XREAD(data[0], command[4], data[1])
                                self.socket.send(signal)
                    else:
                        limit = time() + int(command[2])/1000
                        self.share_data.block_reads.append(limit)
                        with self.share_data.condition:
                            if self.share_data.condition.wait(timeout=int(command[2])/1000):
                                data = self.share_data.stream_log[command[4]][-1]
                                signal = self.block_XREAD(data[0], command[4], data[1])
                                self.socket.send(signal)
                            else:
                                self.socket.send("$-1\r\n".encode())
                    self.share_data.block_reads = []
                else:
                    signal = stream.query_stream_XREAD(command[2:])
                    self.socket.send(signal)
            case "INFO":
                self.socket.send("$11\r\nrole:master\r\n".encode())
                pass
        print("Sent message")