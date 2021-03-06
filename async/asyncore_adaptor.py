import asyncore
import socket

import async


class AsyncoreAdaptor(async.ReadBufferingAdaptor, asyncore.dispatcher):
    def __init__(self, protocol, host, port):
        asyncore.dispatcher.__init__(self)
        self.protocol = protocol
        protocol.adaptor = self

        self._init_read_buffering()
        self._writing = False
        self._write_buffer = ''

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))


    def write(self, data):
        self._writing = True
        self._write_buffer = data


    def is_waiting(self):
        return self._reading or self._writing


    def writable(self):
        return self._writing


    def readable(self):
        return self._reading


    def handle_read(self):
        self._handle_read(self.recv(self._read_length - len(self._read_buffer)))


    def handle_write(self):
        sent = self.send(self._write_buffer)
        self._write_buffer = self._write_buffer[sent:]
        if len(self._write_buffer) == 0:
            # finished writing
            self._writing = False
            self.protocol._write_callback()


    def add_callback(self, callback):
        # FIXME
        callback()
