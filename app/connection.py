from threading import Thread
from time import time
from app.RDB_file_config import RDB_fileconfig
from app.streams import Streams
from app.replication import Replication
import time

class Commands(Thread, RDB_fileconfig, Streams, Replication):
    def __init__(self, socket=None, address=None, dir=None, dbfilename=None, share_data=None, replica=None, replication = None, repClass=None, role="MASTER"):
        super().__init__()
        self.socket = socket
        self.address = address
        self.path = RDB_fileconfig(dir, dbfilename)
        self.store = {}
        self.expiry_time = {}
        self.config_get = {}
        self.share_data = share_data
        self.replica = replica
        self.replication = replication
        self.role = role
        if repClass:
            self.repset = repClass.set
        self.start()

    def run(self):
        while True:
            request = self.socket.recv(4096)
            if request:
                command = self.parse_request(request)
                print("PARSE_REQ", command) 
                self.parse_command(command, request)

    def parse_request(self, request):
        return request.decode().split("\r\n")[2:-1:2]

    def parse_command(self, command, request):
        check_command = command[0].upper()
        match check_command:
            case "PING":
                self.socket.send("+PONG\r\n".encode())
            case "ECHO":
                self.socket.send(f"+{command[1]}\r\n".encode())
            case "SET":
                if self.role == "MASTER":
                    for rep in self.share_data.replication:
                        rep.sendall(request)
                        self.share_data.replication[rep] = 1
                self.store[command[1]] = command[2]
                print("SELF.store", self.store)
                if len(command) > 3 and command[3].lower() == "px":
                    additional_time = int(command[-1])
                    self.expiry_time[command[1]] = additional_time+(time.time()*1000)
                self.socket.send("+OK\r\n".encode())
                print("self.replicatin after set", self.share_data.replication)
            case "GET":
                if self.role == 'SLAVE':
                    value = self.repset[command[1]]
                    self.socket.send(f"${len(value)}\r\n{value}\r\n".encode())
                elif self.path.dir and self.path.filename:
                    signal = self.read_rdb_file(self.path.dir, self.path.filename, "GET")
                    self.socket.send(signal[command[1]])
                    for rep in self.share_data.replication.keys():
                        rep.sendall(request)
                elif command[1] in self.expiry_time and self.expiry_time[command[1]] >= time.time() * 1000 or command[1] not in self.expiry_time:
                    self.socket.send(f"+{self.store[command[1]]}\r\n".encode())
                    for rep in self.share_data.replication.keys():
                        rep.sendall(request)
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
                    if self.share_data.block_reads and self.share_data.block_reads[0] <= time.time()+1:
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
                        limit = time.time() + int(command[2])/1000
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
                if self.role == "SLAVE":
                    self.socket.send("$10\r\nrole:slave\r\n".encode())
                else:
                    signal = self.share_data.initial_replication_ID_and_Offset()
                    self.socket.send(f"${len(signal)}\r\n{signal}\r\n".encode())
            case "REPLCONF":
                print("HMMMMMMMMMMMM")
                self.handle_replconf(command[1])
                # self.socket.send("+OK\r\n".encode())
            case "PSYNC":
                print("PYSNC", request)
                print("HH")
                self.share_data.replication[self.socket] = 0
                self.socket.send("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".encode())
                signal = self.share_data.empty_RDB()
                self.socket.send(signal)
                print("total replicas", self.share_data.replication, len(self.share_data.replication))
            case "WAIT":
           
                # else:
                self.handle_wait(command[1], command[2])
        print("Sent message")


    def handle_wait(self, count, timer):
        print("handle wait")
        for repl, value in self.share_data.replication.items():
            if value == 1:
                print("does this enter")
                repl.send("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".encode("utf-8"))
        
        start = time.time()+0.05
        print("self.share_data.replied_act", self.share_data.replied_ack)
        while self.share_data.replied_ack < int(count) and (time.time() - int(start)) < int(timer) / 1000:
            print("timer hittt")
            time.sleep(0.01)
        print("replied_ack", self.share_data.replied_ack)
        not_acked = len([ repl for repl, val in self.share_data.replication.items() if val == 0])
        print("NOTTTTT", not_acked)
        if not_acked == len(self.share_data.replication):
            print("0000000000000000000")
            response = f":{str(len(self.share_data.replication))}\r\n".encode("utf-8")
        #     self.socket.send(response.encode())
        else:
            print("DPES THI SEVEN HIT")
            response = f":{self.share_data.replied_ack}\r\n".encode("utf-8")
            self.share_data.replied_ack = 0
        self.socket.send(response)

    def handle_replconf(self, command):
        # if self.role == "MASTER":
        print("MASTER?")
        if command == 'ACK':
            print("ADD MASTER")
            self.share_data.replied_ack+=1
        else:
            self.socket.send("+OK\r\n".encode())