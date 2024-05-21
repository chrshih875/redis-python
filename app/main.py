import socket
from threading import *

class Connection(Thread):
    def __init__(self, socket, address):
        super().__init__()
        self.socket = socket
        self.address = address
        self.start()

    def run(self):
        while True:
            # print("Waiting for message")
            request = self.socket.recv(4096)
            if request:
                parse_request = self.parse_request(request)
                # print("PARSE_REQ", parse_request) 
                self.parse_command(parse_request)

    def parse_request(self, request):
        return request.decode().split("\r\n")[2:-1:2]

    def parse_command(self, command):
        check_command = command[0].upper()
        match check_command:
            case "PING":
                self.socket.send("+PONG\r\n".encode())
            case ECHO:
                self.socket.send(f"+{command[1]}\r\n".encode())
        print("adding")
        print("Sent message")

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, address = server_socket.accept() # wait for client
        Connection(conn, address)
        # threading.Thread(target=handle_request, args=(conn, address)).start()



if __name__ == "__main__":
    main()
