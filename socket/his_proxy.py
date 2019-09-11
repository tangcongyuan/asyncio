#! /usr/bin/env python3

import queue
import socket
import typing


class HisProxy:
    _incoming_q: queue.Queue = queue.Queue() # Stores incoming prescriptions
    _outgoing_q: queue.Queue = queue.Queue() # Stores outgoing detected medicines
    _prescriptions: typing.List[str] = []
    _detected_meds: typing.List[str] = []
    _socket: socket.socket = None
    _msg_size: int = 1024

    def __init__(self,
                 host: str='127.0.0.1',
                 port: int=65432) -> None:
        self._host = host
        self._port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self._host, self._port))

    def __del__(self):
        self._socket.close()

    def send(self, data: bytes):
        self._socket.sendall(data)
        received = self._socket.recv(self._msg_size)
        print('Received', repr(received))


# Use this singleton instead of initiating new ones.
proxy = HisProxy()


# For testing only
if __name__ == "__main__":
    proxy.send(b'Hello proxy.')
