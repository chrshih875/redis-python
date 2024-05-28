import socket


class Replication:
    def initial_replication_ID_and_Offset(self):
        master_repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        return f"role:master\nmaster_replid:{master_repl_id}\nmaster_repl_offset:0"
    
    def connect_master(self, host, port, slave_port):
        with socket.create_connection((host, port)) as s:
            s.send("*1\r\n$4\r\nPING\r\n".encode())
            s.recv(1024)
            s.send(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{slave_port}\r\n".encode())
            s.recv(1024)
            s.send("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode())
            s.recv(1024)



