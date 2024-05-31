import socket
import argparse
from app.connection import Commands
from collections import defaultdict
from threading import Condition
from app.replication import Replication

role = "MASTER"
class SharedData(Replication):
    def __init__(self, role):
        super().__init__(role)
        self.stream_log = defaultdict(list)
        self.block_reads = []
        self.condition = Condition()
        self.replication = {}
        self.replied_ack = 0


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
    global role
    shared_data = SharedData(role)
    args = file_config(argparse.ArgumentParser())
    server = None
    port = int(args.port) if args.port else 6379
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    if args.replicaof:
        role = 'SLAVE'
        server = Replication(role)
        master_host, master_port = args.replicaof.split(" ")
        master_conn = socket.socket()
        master_conn.connect((master_host, int(master_port)))
        server.connect_master(master_conn, args.port)
    while True:
        conn, address = server_socket.accept() # wait for client
        Commands(conn, address, args.dir, args.dbfilename, shared_data, replication, args.replicaof, server, role=role)


if __name__ == "__main__":
    main()
