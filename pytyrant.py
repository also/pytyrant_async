"""Pure python implementation of the binary Tokyo Tyrant 1.1.17 protocol

Tokyo Cabinet <http://tokyocabinet.sourceforge.net/> is a "super hyper ultra
database manager" written and maintained by Mikio Hirabayashi and released
under the LGPL.

Tokyo Tyrant is the de facto database server for Tokyo Cabinet written and
maintained by the same author. It supports a REST HTTP protocol, memcached,
and its own simple binary protocol. This library implements the full binary
protocol for the Tokyo Tyrant 1.1.17 in pure Python as defined here::

    http://tokyocabinet.sourceforge.net/tyrantdoc/

Typical usage is with the PyTyrant class which provides a dict-like wrapper
for the raw Tyrant protocol::

    >>> import pytyrant
    >>> t = pytyrant.PyTyrant.open('127.0.0.1', 1978)
    >>> t['__test_key__'] = 'foo'
    >>> t.concat('__test_key__', 'bar')
    >>> print t['__test_key__']
    foobar
    >>> del t['__test_key__']

"""
import asyncore
import math
import socket
import struct
import UserDict

__version__ = '1.1.17'

__all__ = [
    'Tyrant', 'TyrantError', 'PyTyrant',
    'RDBMONOULOG', 'RDBXOLCKREC', 'RDBXOLCKGLB',
]

class TyrantError(Exception):
    pass


DEFAULT_PORT = 1978
MAGIC = 0xc8


RDBMONOULOG = 1 << 0
RDBXOLCKREC = 1 << 0
RDBXOLCKGLB = 1 << 1


class C(object):
    """
    Tyrant Protocol constants
    """
    put = 0x10
    putkeep = 0x11
    putcat = 0x12
    putshl = 0x13
    putnr = 0x18
    out = 0x20
    get = 0x30
    mget = 0x31
    vsiz = 0x38
    iterinit = 0x50
    iternext = 0x51
    fwmkeys = 0x58
    addint = 0x60
    adddouble = 0x61
    ext = 0x68
    sync = 0x70
    vanish = 0x71
    copy = 0x72
    restore = 0x73
    setmst = 0x78
    rnum = 0x80
    size = 0x81
    stat = 0x88
    misc = 0x90


def _t0(code):
    return [chr(MAGIC) + chr(code)]


def _t1(code, key):
    return [
        struct.pack('>BBI', MAGIC, code, len(key)),
        key,
    ]


def _t1FN(code, func, opts, args):
    outlst = [
        struct.pack('>BBIII', MAGIC, code, len(func), opts, len(args)),
        func,
    ]
    for k in args:
        outlst.extend([struct.pack('>I', len(k)), k])
    return outlst


def _t1R(code, key, msec):
    return [
        struct.pack('>BBIQ', MAGIC, code, len(key), msec),
        key,
    ]


def _t1M(code, key, count):
    return [
        struct.pack('>BBII', MAGIC, code, len(key), count),
        key,
    ]


def _tN(code, klst):
    outlst = [struct.pack('>BBI', MAGIC, code, len(klst))]
    for k in klst:
        outlst.extend([struct.pack('>I', len(k)), k])
    return outlst


def _t2(code, key, value):
    return [
        struct.pack('>BBII', MAGIC, code, len(key), len(value)),
        key,
        value,
    ]


def _t2W(code, key, value, width):
    return [
        struct.pack('>BBIII', MAGIC, code, len(key), len(value), width),
        key,
        value,
    ]


def _t3F(code, func, opts, key, value):
    return [
        struct.pack('>BBIIII', MAGIC, code, len(func), opts, len(key), len(value)),
        func,
        key,
        value,
    ]


def _tDouble(code, key, integ, fract):
    return [
        struct.pack('>BBIQQ', MAGIC, code, len(key), integ, fract),
        key,
    ]


def socksend(sock, lst):
    sock.sendall(''.join(lst))


def sockrecv(sock, bytes):
    d = ''
    while len(d) < bytes:
        d += sock.recv(min(8192, bytes - len(d)))
    return d


def socksuccess(sock):
    fail_code = ord(sockrecv(sock, 1))
    if fail_code:
        raise TyrantError(fail_code)


def socklen(sock):
    return struct.unpack('>I', sockrecv(sock, 4))[0]


def socklong(sock):
    return struct.unpack('>Q', sockrecv(sock, 8))[0]


def sockstr(sock):
    return sockrecv(sock, socklen(sock))


def sockdouble(sock):
    intpart, fracpart = struct.unpack('>QQ', sockrecv(sock, 16))
    return intpart + (fracpart * 1e-12)


def sockstrpair(sock):
    klen = socklen(sock)
    vlen = socklen(sock)
    k = sockrecv(sock, klen)
    v = sockrecv(sock, vlen)
    return k, v


class Tyrant(asyncore.dispatcher):
    def __init__(self, host='127.0.0.1', port=DEFAULT_PORT):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))
        #self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self._q = []
        self._reading = False
        self._writing = False
        self._write_buffer = ''
        self._read_length = 0
        self._read_buffer = ''
        self._use_result_as_args = False
        self._call_complete = False


    def handle_connect(self):
        pass


    def handle_close(self):
        self.close()


    def handle_error(self):
        import traceback
        traceback.print_exc()
        self.close()


    def writable(self):
        if self._writing:
            return True
        else:
            return False


    def readable(self):
        if self._reading:
            return True
        else:
            return False


    def handle_read(self):
        self._read_buffer += self.recv(self._read_length - len(self._read_buffer))
        if len(self._read_buffer) == self._read_length:
            # finished reading
            self._reading = False
            self._work()


    def handle_write(self):
        sent = self.send(self._write_buffer)
        self._write_buffer = self._write_buffer[sent:]
        if len(self._write_buffer) == 0:
            # finished writing
            self._writing = False
            self._work()


    def _is_waiting(self):
        return self._reading or self._writing


    def _do_now(self, *steps):
        self._q = list(steps) + self._q
        self._work()


    def _do(self, steps, callback=None):
        if type(steps) is not list:
            steps = [steps]
        self._q.extend(steps)
        self._callback = callback
        self._call_complete = False
        self._work()


    def _work(self):
        while not (self._call_complete or self._is_waiting()):
            self._advance()


    def _advance(self):
        '''
        Executes the next function in the queue, or calls the callback with the result.
        '''
        self._working = True
        
        if len(self._q) == 0:
            self._complete(self._result)
            return
        else:
            action = self._q.pop(0)

        if type(action) is tuple:
            try:
                fn, args = action
            except:
                print repr(action)
                raise
        else:
            fn = action
            args = []
        if self._use_result_as_args:
            args.append(self._result)
            self._use_result_as_args = False
        fn(*args)


    def _fail(self):
        # TODO
        self._complete(None)


    def _complete(self, callback_value):
        self._call_complete = True
        self._q = []
        self._callback(callback_value)


    def _read(self, length):
        self._reading = True
        self._read_buffer = ''
        self._read_length = length


    def _write(self, lst):
        self._writing = True
        self._write_buffer = ''.join(lst)


    def _use_result(self):
        self._use_result_as_args = True


    def _use_read_buffer_as_result(self):
        self._set_result(self._read_buffer)


    def _str(self):
        '''
        Reads a string from the server and stores it in the result.
        '''
        self._do_now(
            self._len,  # length -> _result
            self._use_result,
            self._read,
            self._use_read_buffer_as_result
        )

    def _len(self):
        '''
        Reads a length from the server and stores it in the result.
        '''
        self._do_now(
            (self._read, [4]),
            self._process_read_buffer(lambda result: struct.unpack('>I', result)[0])
        )


    def _set_result(self, result):
        self._result = result


    def _process_read_buffer(self, callback):
        return lambda: self._set_result(callback(self._read_buffer))


    def _success(self):
        '''
        Reads the response from the server.
        
        On success, sets result to True. On failure, calls the failure callback.
        '''
        def _check_result():
            if ord(self._read_buffer):
                self._fail()
            else:
                self._set_result(True)
        self._do_now(
            (self._read, [1]),
            _check_result
        )


    def get(self, key, callback):
        """Get the value of a key from the server
        """
        self._do([
            (self._write, [_t1(C.get, key)]),
            self._success,
            self._str
        ], callback)


    def put(self, key, value, callback):
        self._do([
            (self._write, [_t2(C.put, key, value)]),
            self._success
        ], callback)


def callback(*args):
    print '------- callback --------'
    print repr(args)
