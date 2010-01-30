class StreamProtocol(object):
    def __init__(self):
        self._cmds = []
        self._q = []
        self._use_result_as_args = False
        self._call_active = False


    def close(self):
        return self.adaptor.close()


    def read(self, data):
        return self.adaptor.read(data)


    def write(self, buffer_size):
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
        while self._call_active and not self.adaptor._is_waiting():
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
