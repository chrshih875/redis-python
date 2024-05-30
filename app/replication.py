import socket
import time
import threading
# from app.connection import Commands
class Replication:
    def __init__(self, master_repl_id='8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb', offset=0):
        super().__init__()
        self.master_repl_id = master_repl_id
        self.offset = offset
        self.set = {}
        # self.command = Commands(None, None, None, None, None, None, None)

    def initial_replication_ID_and_Offset(self):
        return f"role:master\nmaster_replid:{self.master_repl_id}\nmaster_repl_offset:{self.offset}"
    
    def connect_master(self, conn, slave_port):
        # with socket.create_connection((host, port)) as s:
        # testing = ["*1\r\n$4\r\nPING\r\n".encode(), f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{slave_port}\r\n".encode(), "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode(), "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode()]
        try:
            conn.send("*1\r\n$4\r\nPING\r\n".encode())
            conn.recv(1024)
            conn.send(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{slave_port}\r\n".encode())
            conn.recv(1024)
            conn.send("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode())
            conn.recv(1024)
            conn.send("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())
            conn.recv(1024)
            data = conn.recv(1024)
            print("outsideee", data)
            master_thread = threading.Thread(
            target=self.recieve_command,
            args=(conn,),
            daemon=True,
        ).start()
        except Exception as e:
            print(f"An error occurred: {e}")

    def recieve_command(self, conn):
        print("not hit")
        while True:
            request = conn.recv(1024)
            if request:
                print("request lentyhgggg", len(request))
                command = self.parse_request(request)
                print("command", command)
                if command[-1][0] == "REPLCONF" and len(command) > 1:
                    self.offset+=len(request) - 37
                    self.parse_command(command, request, conn)
                else:
                    self.parse_command(command, request, conn)
                    self.offset+=len(request)
                
                print("AFTER EVERYTHIG", self.offset)

    def parse_request(self, data):
        array = []
        current_data = data.decode().split("\r\n")[2:-1:1]
        print("current_dayta", current_data)
        for i in range(len(current_data)):
            if current_data[i] == "SET":
                array.append(current_data[i:i+5])
            if current_data[i] == "REPLCONF":
                array.append(current_data[i:i+5])
            if current_data[i] == "PING":
                array.append([current_data[i]])
        return array
    
    def parse_command(self, command, request, master_conn):
        for val in command:
            if val[0] == "SET":
                self.set[val[2]] = val[-1]
            if val[0] == "REPLCONF":
                new = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(self.offset))}\r\n{self.offset}\r\n".encode()
                master_conn.send(new)
        print("self.set", self.set)

        
    def empty_RDB(self):
        rdb_hex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
        rdb_content = bytes.fromhex(rdb_hex)
        rdb_length = f"${len(rdb_content)}\r\n".encode()
        return rdb_length+rdb_content





