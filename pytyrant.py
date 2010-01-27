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


class PyTyrant(object, UserDict.DictMixin):
    """
    Dict-like proxy for a Tyrant instance
    """
    @classmethod
    def open(cls, *args, **kw):
        return cls(Tyrant.open(*args, **kw))

    def __init__(self, t):
        self.t = t

    def __repr__(self):
        # The __repr__ for UserDict.DictMixin isn't desirable
        # for a large KV store :)
        return object.__repr__(self)

    def has_key(self, key):
        return key in self

    def __contains__(self, key):
        try:
            self.t.vsiz(key)
        except TyrantError:
            return False
        else:
            return True

    def setdefault(self, key, value):
        try:
            self.t.putkeep(key, value)
        except TyrantError:
            return self[key]
        return value

    def __setitem__(self, key, value):
        self.t.put(key, value)

    def __getitem__(self, key):
        try:
            return self.t.get(key)
        except TyrantError:
            raise KeyError(key)

    def __delitem__(self, key):
        try:
            self.t.out(key)
        except TyrantError:
            raise KeyError(key)

    def __iter__(self):
        return self.iterkeys()

    def iterkeys(self):
        self.t.iterinit()
        try:
            while True:
                yield self.t.iternext()
        except TyrantError:
            pass

    def keys(self):
        return list(self.iterkeys())

    def __len__(self):
        return self.t.rnum()

    def clear(self):
        self.t.vanish()

    def update(self, other=None, **kwargs):
        # Make progressively weaker assumptions about "other"
        if other is None:
            pass
        elif hasattr(other, 'iteritems'):
            self.multi_set(other.iteritems())
        elif hasattr(other, 'keys'):
            self.multi_set([(k, other[k]) for k in other.keys()])
        else:
            self.multi_set(other)
        if kwargs:
            self.update(kwargs)

    def multi_del(self, keys, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)
        self.t.misc("outlist", opts, keys)

    def multi_get(self, keys, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)
        rval = self.t.misc("getlist", opts, keys)
        if len(rval) <= len(keys):
            # 1.1.10 protocol, may return invalid results
            if len(rval) < len(keys):
                raise KeyError("Missing a result, unusable response in 1.1.10")
            return rval
        # 1.1.11 protocol returns interleaved key, value list
        d = dict((rval[i], rval[i + 1]) for i in xrange(0, len(rval), 2))
        return map(d.get, keys)

    def multi_set(self, items, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        lst = []
        for k, v in items:
            lst.extend((k, v))
        self.t.misc("putlist", opts, lst)

    def call_func(self, func, key, value, record_locking=False, global_locking=False):
        opts = (
            (record_locking and RDBXOLCKREC or 0) |
            (global_locking and RDBXOLCKGLB or 0))
        return self.t.ext(func, opts, key, value)

    def get_size(self, key):
        try:
            return self.t.vsiz(key)
        except TyrantError:
            raise KeyError(key)

    def get_stats(self):
        return dict(l.split('\t', 1) for l in self.t.stat().splitlines() if l)

    def prefix_keys(self, prefix, maxkeys=None):
        if maxkeys is None:
            maxkeys = len(self)
        return self.t.fwmkeys(prefix, maxkeys)

    def concat(self, key, value, width=None):
        if width is None:
            self.t.putcat(key, value)
        else:
            self.t.putshl(key, value, width)

    def sync(self):
        self.t.sync()

    def close(self):
        self.t.close()


class Tyrant(asyncore.dispatcher):
    def __init__(self, host='127.0.0.1', port=DEFAULT_PORT):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self._q = []
        self._reading = False
        self._writing = False
        self._write_buffer = ''
        self._read_length = 0
        self._read_buffer = ''
        self._working = False
        self._use_result_as_args = False


    def writable(self):
        if self._writing:
            if len(self._write_buffer) > 0:
                return True
            else:
                self._writing = False
                self._advance()
                return False
        else:
            return False


    def readable(self):
        if self._reading:
            if len(self._read_buffer) < self._read_length:
                return True
            else:
                self._reading = False
                self._advance()
                return False
        else:
            return False


    def handle_read(self):
        self._read_buffer += self.recv(self._read_length - len(self._read_buffer))


    def handle_write(self):
        sent = self.send(self._write_buffer)
        self.buffer = self._write_buffer[sent:]


    def _do_now(self, steps):
        if not type(steps) is list:
            steps = [steps]
        self._q. = steps.extend(self._q)
        # TODO advance?


    def _do(self, steps, callback=None):
        if type(steps) is not list:
            steps = [steps]
        self._q.extend(list)
        if callback is not None:
            self._q.append((self._call_callback, callback))
        if not self._working:
            self._advance()


    def _advance(self):
        self._working = True
        if len(self._q) > 0:
            action = self._q.pop()
        else:
            raise Exception('cannot advance')

        if type(action) is tuple:
            fn, args = action
        else:
            fn = action
            args = []
        if self._use_result_as_args:
            args.append(self._result)
            self._use_result = False
        fn(*args)


    def _call_callback(self, callback):
        callback(self._result)


    def _read(self, length):
        self._reading = True
        self._read_buffer = ''
        self._read_length = length


    def _write(self, lst):
        self._writing = True
        self._write_buffer = ''.join(lst)


    def _use_result(self):
        self._use_result_as_args = True


    def _str(self):
        self._do_now(
            self._len,  # length -> _result
            self._use_result,
            self.read,
            self._use_read_buffer_as_result
        )


    def _set_result(self, result):
        self._result = result


    def _process_read_buffer(self, callback):
        self._do_now(lambda: self._set_result(callback(self._read_buffer)))


    def _success(self):
        self._read(1)
        self._process_read_buffer(lambda result: not ord(result))

    def get(self, key, callback):
        """Get the value of a key from the server
        """
        # socksend(self.sock, _t1(C.get, key))
        # socksuccess(self.sock)
        # return sockstr(self.sock)
        self._do([
            (self._write, _t1(C.get, key)),
            self._success,
            self._str
        ], callback)
