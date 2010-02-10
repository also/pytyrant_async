from twisted.internet import protocol

import async


class TwistedAdaptor():
    def __init__(self, protocol, host, port):
        reactor.connectTCP(host, port, TwistedFactory(protocol, self))
        protocol.adaptor = self
        self._twisted = None
        self._write_buffer = None
        self._read_length = None


    def is_waiting(self):
        return (self._write_buffer is not None or self._read_length is not None) or (self._twisted and self._twisted._reading)


    def read(self, length):
        if self._read_length is not None:
            raise Exception('read called again before callback')

        if self._twisted is None:
            self._read_length = length
        else:
            self._twisted.read(length)


    def write(self, data):
        if self._write_buffer is not None:
            raise Exception('write called again before callback')

        if self._twisted is None:
            self._write_buffer = data
        else:
            self._twisted.write(data)


class TwistedFactory(protocol.ClientFactory):
    def __init__(self, protocol, adaptor):
        self.protocol = protocol
        self.adaptor = adaptor


    def buildProtocol(self, addr):
        return TwistedProtocol(self.protocol, self.adaptor)


class TwistedProtocol(async.ReadBufferingAdaptor, protocol.Protocol):
    def __init__(self, protocol, adaptor):
        self.protocol = protocol
        self.adaptor = adaptor
        adaptor._twisted = self
        self._init_read_buffering()
        self.transport = None


    def connectionMade(self):
        if self.adaptor._write_buffer is not None:
            write_buffer = self.adaptor._write_buffer
            self.adaptor._write_buffer = None
            self.write(write_buffer)
        if self.adaptor._read_length is not None:
            read_length = self.adaptor._read_length
            self.adaptor._read_length = None
            self.read(read_length)


    def dataReceived(self, data):
        self._handle_read(data)


    def write(self, data):
        self.transport.write(data)
        # TODO
        self.protocol._write_callback()
