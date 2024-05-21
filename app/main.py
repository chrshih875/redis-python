import socket
import threading

def handle_request(connection, address):
    with connection:
        while True:
            print("Waiting for message")
            data = connection.recv(4096).decode()
            if not data:
                break
            print("DATA", data)
            command = data[0].upper()
            match command:
                case "PING":
                    connection.send("+PONG\r\n".encode())
                case ECHO:
                    connection.send(data[1].encode())

            print("Sent message")


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("testing this commint went through")
    while True:
        conn, address = server_socket.accept() # wait for client
        threading.Thread(target=handle_request, args=(conn, address)).start()



if __name__ == "__main__":
    main()
