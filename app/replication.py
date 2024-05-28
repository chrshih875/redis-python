import asyncio
import socket


class Replication:
    # def __init__(self, role='master', replication_ID='8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb', offset=0):
    #     self.role = role
    #     self.replication_ID = replication_ID
    #     self.offset = offset
    #     pass

    def initial_replication_ID_and_Offset(self):
        master_repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        return f"role:master\nmaster_replid:{master_repl_id}\nmaster_repl_offset:0"
    
    def connect_master(self, host, port):
        with socket.create_connection((host, port)) as s:
            s.send("*1\r\n$4\r\nPING\r\n".encode())
        # writer.write("*1\r\n$4\r\nPING\r\n")
        # await writer.drain()

    def listen(self, master_host, master_port, slave_port):
        # await self.connect_master(master_host, master_port)
        # print("hell1o")
        # await asyncio.start_server(master_host, master_port)
        # print("hello")
        pass