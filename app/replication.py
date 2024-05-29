import socket


class Replication:
    def __init__(self, master_repl_id='8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb', offset='0'):
        self.master_repl_id = master_repl_id
        self.offset = offset

    def initial_replication_ID_and_Offset(self):
        return f"role:master\nmaster_replid:{self.master_repl_id}\nmaster_repl_offset:{self.offset}"
    
    def connect_master(self, host, port, slave_port):
        with socket.create_connection((host, port)) as s:
            s.send("*1\r\n$4\r\nPING\r\n".encode())
            s.recv(1024)
            s.send(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{slave_port}\r\n".encode())
            s.recv(1024)
            s.send("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode())
            s.recv(1024)
            s.send("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())
            s.recv(1024)
    
    def empty_RDB(self):
        rdb_hex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
        rdb_content = bytes.fromhex(rdb_hex)
        rdb_length = f"${len(rdb_content)}\r\n".encode()
        return rdb_length+rdb_content





