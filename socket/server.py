#! /usr/bin/env python3

import socket
import time

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 65432        # Port to listen on (non-privileged ports are > 1023)
MSG_SIZE = 1024     # 1KB for socket data transmition

def server_start():
    # socket.AF_INET => IPv4; socket.SOCK_STREAM => TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        conn, addr = s.accept()
        with conn:
            print('Connected by', addr)
            while True:
                try:
                    data = conn.recv(MSG_SIZE)
                    print('Received', repr(data))
                    if not data: break
                    conn.sendall(data)
                except KeyboardInterrupt:
                    print('Stoping server.')
                    break


if __name__ == "__main__":
    print('Server starts.')
    server_start()