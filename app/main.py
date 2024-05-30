import socket
import argparse
from app.connection import Commands
from collections import defaultdict
from threading import Condition
import threading
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
    role = 'MASTER'
    print("args", args)
    server = None
    port = int(args.port) if args.port else 6379
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    if args.replicaof:
        role = 'SLAVE'
        server = Replication()
        master_host, master_port = args.replicaof.split(" ")
        master_conn = socket.socket()
        master_conn.connect((master_host, int(master_port)))
        server.connect_master(master_conn, args.port)
        # print("master_thread1111")
        # master_thread = threading.Thread(
        #     target=server.recieve_command,
        #     args=(master_conn,),
        #     daemon=True,
        # ).start()
        # server.recieve_command(master_conn)
        # master_conn.close()
    # else:
    while True:
        conn, address = server_socket.accept() # wait for client
        Commands(conn, address, args.dir, args.dbfilename, shared_data, replication, args.replicaof, server, role=role)


if __name__ == "__main__":
    main()
