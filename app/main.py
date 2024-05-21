import socket
import threading

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")


    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, address = server_socket.accept() # wait for client
    threading.Thread(target=handle_request, args=conn).start()

def handle_request(connection):
    with connection:
        while True:
            print("Waiting for message")
            data = connection.recv(4096).decode()
            print(data)
            connection.send("+PONG\r\n".encode())
            print("Sent message")


if __name__ == "__main__":
    main()
