import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")


    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, address = server_socket.accept() # wait for client
    with conn:
        while True:
            print("Waiting for message")
            data = conn.recv(4096).decode()
            print(data)
            conn.send("+PONG\r\n".encode())
            print("Sent message")
    print("con", conn)


if __name__ == "__main__":
    main()
