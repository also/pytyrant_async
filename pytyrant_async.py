import struct

import async

__version__ = '1.1.17'

__all__ = [
    'TyrantError', 'Tyrant',
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


# def sockdouble(sock):
#     intpart, fracpart = struct.unpack('>QQ', sockrecv(sock, 16))
#     return intpart + (fracpart * 1e-12)
#
#
# def sockstrpair(sock):
#     klen = socklen(sock)
#     vlen = socklen(sock)
#     k = sockrecv(sock, klen)
#     v = sockrecv(sock, vlen)
#     return k, v


class Tyrant(async.StreamProtocol):
    def handle_error(self):
        import traceback
        traceback.print_exc()
        self.close()


    def handle_connect(self):
        pass


    def handle_close(self):
        self.close()


    def _write(self, lst):
        super(Tyrant, self)._write(''.join(lst))


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
            (self._read, 4),
            self._process_read_buffer(lambda result: struct.unpack('>I', result)[0])
        )


    def _long(self):
        '''
        Reads a long from the server and stores it in the result.
        '''
        self._do_now(
            (self._read, 8),
            self._process_read_buffer(lambda result: struct.unpack('>Q', result)[0])
        )


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
            (self._read, 1),
            _check_result
        )


    def get(self, key, callback):
        self._do([
            (self._write, _t1(C.get, key)),
            self._success,
            self._str
        ], callback)


    def put(self, key, value, callback):
        self._do([
            (self._write, _t2(C.put, key, value)),
            self._success
        ], callback)


    def out(self, key, callback):
        self._do([
            (self._write, _t1(C.out, key)),
            self._success
        ], callback)


    def rnum(self, callback):
        self._do([
            (self._write, _t0(C.rnum)),
            self._success,
            self._long,
        ], callback)


    def size(self, callback):
        self._do([
            (self._write, _t0(C.size)),
            self._success,
            self._long,
        ], callback)


    def stat(self, callback):
        self._do([
            (self._write, _t0(C.stat)),
            self._success,
            self._str
        ], callback)


class YTyrant(async.YStreamProtocol):
    def _write(self, lst):
        return super(YTyrant, self)._write(''.join(lst))


    @async.y_helper
    def _len(self):
        result = yield self._read(4)
        async.y_return(struct.unpack('>I', result)[0])


    @async.y_helper
    def _success(self):
        result = yield self._read(1)
        if ord(result):
            raise TyrantError


    @async.y_helper
    def _long(self):
        '''
        Reads a long from the server.
        '''
        result = yield self._read(8)
        async.y_return(struct.unpack('>Q', result)[0])


    @async.y_helper
    def _str(self):
        str_len = yield self._len()
        result = yield self._read(str_len)
        async.y_return(result)


    @async.y
    def get(self, key):
        yield self._write(_t1(C.get, key))
        yield self._success()
        result = yield self._str()
        async.y_return(result)


    @async.y
    def put(self, key, value):
        yield self._write(_t2(C.put, key, value))
        result = yield self._success()
        async.y_return(result)


    @async.y
    def rnum(self):
        yield self._write(_t0(C.rnum))
        yield self._success()
        result = yield self._long()
        async.y_return(result)


def callback(*args):
    print '------- callback --------'
    print ',\n'.join(map(repr, args))
