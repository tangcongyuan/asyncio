#! /usr/bin/env python3

import asyncio
import time
import socket

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 65432        # The port used by the server
MSG_SIZE = 1024     # 1KB for socket data transmition


async def async_client_start(message, loop):
    reader, writer = await asyncio.open_connection(HOST, PORT,
                                                   loop=loop)

    writer.write(message.encode())
    print('Send: %r' % message)

    data = await reader.read(MSG_SIZE)
    print('Received: %r' % data.decode())

    print('Close the socket')
    writer.close()


if __name__ == "__main__":
    print('Asyncio client starts.')
    message = 'Hello World!'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_client_start(message, loop))
    loop.close()
