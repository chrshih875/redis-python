import socket
import argparse
from app.connection import Connection
from collections import defaultdict
from threading import Condition, Lock

class SharedData:
    def __init__(self):
        self.stream_log = defaultdict(list)
        self.block_reads = []
        self.condition = Condition()

def file_config(command_line):
    command_line.add_argument('--dir', required=False, help='Directory for Redis files')
    command_line.add_argument('--dbfilename', required=False, help='Filename for the Redis database')
    args = command_line.parse_args()
    return args

def main():
    print("Logs from your program will appear here!")
    shared_data = SharedData()
    args = file_config(argparse.ArgumentParser())
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, address = server_socket.accept() # wait for client
        Connection(conn, address, args.dir, args.dbfilename, shared_data)

if __name__ == "__main__":
    main()
