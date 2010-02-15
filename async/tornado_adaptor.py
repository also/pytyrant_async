import socket

from tornado import iostream


class TornadoAdaptor(object):
    def __init__(self, protocol, host, port):
        self.protocol = protocol
        protocol.adaptor = self
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        s.connect((host, port))
        self.stream = iostream.IOStream(s)

        self.add_callback = self.stream.io_loop.add_callback


    def is_waiting(self):
        return self.stream.reading() or self.stream.writing()


    def read(self, length):
        self.stream.read_bytes(length, self.protocol._read_callback)


    def write(self, data):
        self.stream.write(data, self.protocol._write_callback)
