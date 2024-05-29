import socket
import argparse
from app.connection import Commands
from collections import defaultdict
from threading import Condition
from app.replication import Replication
class SharedData(Replication):
    def __init__(self):
        super().__init__()
        self.stream_log = defaultdict(list)
        self.block_reads = []
        self.condition = Condition()

def file_config(command_line):
    command_line.add_argument('--dir', required=False, help='Directory for Redis files')
    command_line.add_argument('--dbfilename', required=False, help='Filename for the Redis database')
    command_line.add_argument('--port', required=False, help='Listening for custom port')
    command_line.add_argument('--replicaof', required=False, help='slaveport')
    args = command_line.parse_args()
    return args

def main():
    print("Logs from your program will appear here!")
    replication = []
    shared_data = SharedData()
    args = file_config(argparse.ArgumentParser())
    print("args", args)

    port = int(args.port) if args.port else 6379
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    # if args.port:
    if args.replicaof:
        print("replica TRUE")
        server = Replication()
        master_host, master_port = args.replicaof.split(" ")
        server.connect_master(master_host, master_port, args.port)
        # server.connect_master("localhost", args.port, args.port)


    while True:
        conn, address = server_socket.accept() # wait for client
        print("hello")
        Commands(conn, address, args.dir, args.dbfilename, shared_data, replication, args.replicaof)


if __name__ == "__main__":
    main()
