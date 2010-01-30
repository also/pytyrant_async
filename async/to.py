import socket

from tornado import iostream

class IOStreamAdaptor(object):
    def __init__(self, protocol, host, port):
        self.protocol = protocol
        protocol.adaptor = self
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        s.connect((host, port))
        self.stream = iostream.IOStream(s)
    
    
    def _is_waiting(self):
        return self.stream.reading() or self.stream.writing()

    
    def read(self, length):
        self.stream.read_bytes(length, self.protocol._read_callback)
    
    
    def write(self, data):
        self.stream.write(data, self.protocol._write_callback)


from tornado import ioloop
start = ioloop.IOLoop.instance().start