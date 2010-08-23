import collections
import functools
import inspect


class StreamProtocol(object):
    def __init__(self):
        self._cmds = []
        self._q = []
        self._use_result_as_args = False
        self._call_active = False


    def close(self):
        return self.adaptor.close()


    def _read(self, data):
        return self.adaptor.read(data)


    def _write(self, buffer_size):
        return self.adaptor.write(buffer_size)


    def _read_callback(self, buffer):
        self._read_buffer = buffer
        self._work()


    def _write_callback(self):
        self._work()


    def _do_now(self, *steps):
        self._q = list(steps) + self._q
        self._work()


    def _do(self, steps, callback=None):
        if type(steps) is not list:
            steps = [steps]
        self._cmds.append((steps, callback))
        if not self._call_active:
            self._advance_cmd()


    def _advance_cmd(self):
        steps, callback = self._cmds.pop(0)
        self._q.extend(steps)
        self._callback = callback
        self._call_active = True
        self._work()


    def _work(self):
        while self._call_active and not self.adaptor.is_waiting():
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
            fn = action[0]
            args = list(action[1:])
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
        self._call_active = False
        self._q = []
        self._callback(callback_value)
        if len(self._cmds) > 0:
            self._advance_cmd()


    def _use_result(self):
        self._use_result_as_args = True


    def _use_read_buffer_as_result(self):
        self._set_result(self._read_buffer)


    def _set_result(self, result):
        self._result = result


    def _process_read_buffer(self, callback):
        return lambda: self._set_result(callback(self._read_buffer))


class ReadBufferingAdaptor(object):
    def _init_read_buffering(self):
        self._reading = False
        self._read_length = 0
        self._read_buffer = ''


    def read(self, length):
        self._reading = True
        self._read_length = length
        self._check_read_buffer()


    def _check_read_buffer(self):
        if len(self._read_buffer) >= self._read_length:
            # finished reading
            self._reading = False
            result = self._read_buffer[:self._read_length]
            self._read_buffer = self._read_buffer[self._read_length:]
            self.protocol._read_callback(result)


    def _handle_read(self, data):
        self._read_buffer += data
        self._check_read_buffer()


def y(coroutine, queue=True):
    argspec = inspect.getargspec(coroutine)
    num_args = len(argspec.args)

    @functools.wraps(coroutine)
    def run(*args, **kwargs):
        # TODO shouldn't depend on length of arguments
        if len(args) == num_args + 1:
            callback = args[-1]
            args = args[:num_args]
        else:
            callback = None

        protocol = args[0]
        adaptor = protocol.adaptor

        def queue_advance():
            if not queue:
                return False
            try:
                next_coroutine = protocol._y_queue.pop()
            except IndexError:
                protocol._y_active = False
                return None
            next_coroutine()


        def run_coroutine():
            if queue:
                protocol._y_active = True

            generator = coroutine(*args, **kwargs)

            if not inspect.isgenerator(generator):
                # TODO warn
                return generator
            else:
                return GeneratorCallback(adaptor, generator, callback, queue_advance)._run()

        if queue:
            if protocol._y_active:
                protocol._y_queue.appendleft(run_coroutine)
                return

        return run_coroutine()

    return run


def y_helper(func):
    y_func = y(func, False)
    @functools.wraps(func)
    def run(*args, **kwargs):
        return functools.partial(y_func, *args, **kwargs)

    run.raw = y_func
    return run


def y_return(*args):
    raise StopIteration(*args)


class GeneratorCallback(object):
    def __init__(self, adaptor, generator, callback, next_callback):
        self._adaptor = adaptor
        self._generator = generator
        self._callback = callback
        self._next_callback = next_callback


    def _run(self):
        try:
            self._work = self._generator.next()
        except StopIteration:
            self._next_callback()
            return None
        self._adaptor.add_callback(self._next)
        return self


    def _next(self):
        return self._work(self._callback_proxy)


    def _callback_proxy(self, *args):
        try:
            if len(args) == 0:
                self._work = self._generator.next()
            elif len(args) == 1:
                self._work = self._generator.send(*args)
            else:
                self._work = self._generator.send(args)

            self._adaptor.add_callback(self._next)
        except StopIteration, e:
            try:
                if self._callback is not None:
                    if len(e.args) == 0:
                        self._callback()
                    elif len(e.args) == 1:
                        self._callback(e.args[0])
                    else:
                        self._callback(e.args)
            finally:
                self._next_callback()
        except Exception, e:
            raise e


class YStreamProtocol(object):
    def __init__(self):
        self._read_queue = collections.deque()
        self._read_cb = None
        self._write_queue = collections.deque()
        self._write_cb = None

        self._y_queue = collections.deque()
        self._y_active = False


    def _read(self, data):
        return lambda callback: self._queue_read(data, callback)

    def _queue_read(self, data, callback):
        if self._read_cb is None:
            self._read_cb = callback
            self.adaptor.read(data)
        else:
            self._read_queue.appendleft((data, callback))


    def _next_read(self):
        try:
            data, callback = self._read_queue.pop()
        except IndexError:
            return
        self._read_cb = callback
        self.adaptor.read(data)


    def _read_callback(self, *args):
        try:
            self._read_cb(*args)
        finally:
            self._read_cb = None
            self._next_read()


    def _write(self, buffer_size):
        return lambda callback: self._queue_write(buffer_size, callback)


    def _queue_write(self, buffer_size, callback):
        if self._write_cb is None:
            self._write_cb = callback
            self.adaptor.write(buffer_size)
        else:
            self._write_queue.appendleft((buffer_size, callback))


    def _next_write(self):
        try:
            buffer_size, callback = self._write_queue.pop()
        except IndexError:
            return
        self._write_cb = callback
        self.adaptor.write(buffer_size)


    def _write_callback(self, *args):
        try:
            self._write_cb(*args)
        finally:
            self._write_cb = None
            self._next_write()
