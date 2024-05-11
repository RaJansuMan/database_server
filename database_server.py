from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
from collections import namedtuple
from io import BytesIO
from socket import error as socket_error


# Data-type	       Prefix	Structure	                  Example
# Simple string    	+	 +{string data}\r\n	               +this is a simple string\r\n

# Error	            –	 -{error message}\r\n               -ERR unknown command "FLUHS"\r\n

# Integer	        :	 :{the number}\r\n	               :1337\r\n

# Binary	        $	${number of bytes}\r\n          	$6\r\n
#                               {data}\r\n                  foobar\r\n

# Array          	*	*{number of elements}\r\n          *3\r\n
#                        {0 or more of above}\r\n          +a simple string element\r\n
#                                                         :12345\r\n
#                                                           $7\r\n
#                                                          testing\r\n
#

# Dictionary	     %	%{number of keys}\r\n{0 or        %3\r\n
#                              more of above}\r\n          +key1\r\n
#                                                         +value1\r\n
#                                                         +key2\r\n
#                                                         *2\r\n
#                                                         +value2-0\r\n
#                                                         +value2-1\r\n
#                                                         :3\r\n
#                                                         $7\r\n
#                                                         testing\r\n
#

# NULL	$	$-1\r\n (string of length -1)


class CommandError(Exception): pass


class Disconnect(Exception): pass


Error = namedtuple('Error', ('message',))


class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict}

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect
        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('bad request')

    def handle_simple_string(self, socket_file):
        return str(socket_file.readline().decode('utf-8')).rstrip('\r\n')

    def handle_error(self, socket_file):
        return Error(str(socket_file.readline().decode('utf-8')).rstrip('\r\n'))

    def handle_integer(self, socket_file):
        return int(str(socket_file.readline().decode('utf-8')).rstrip('\r\n'))

    def handle_string(self, socket_file):
        length = int(str(socket_file.readline().decode('utf-8')).rstrip('\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2]

    def handle_array(self, socket_file):
        num_elements = int(str(socket_file.readline().decode('utf-8')).rstrip('\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = int(str(socket_file.readline().decode('utf-8')).rstrip('\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def _write(self, buf, data):
        if isinstance(data, str):
            buf.write(f'+{data}\r\n'.encode('utf-8'))
        elif isinstance(data, bytes):
            buf.write(f'${len(data)}\r\n{data}\r\n'.encode('utf-8'))
        elif isinstance(data, int):
            buf.write(f':{data}\r\n'.encode('utf-8'))
        elif isinstance(data, Error):
            buf.write(f'-{Error.message}\r\n'.encode('utf-8'))
        elif isinstance(data, (list, tuple)):
            buf.write(f'*{len(data)}\r\n'.encode('utf-8'))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(f'%%{len(data)}\r\n'.encode('utf-8'))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write('$-1\r\n'.encode('utf-8'))
        else:
            raise CommandError('Unrecognized type %s' % type(data))


class Server(object):
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host, port),
            self.connection_handler,
            spawn=self._pool)
        self._protocol = ProtocolHandler()
        self._kv = {}
        self._commands = self.get_commands()

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return 1

    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, *keys):
        return [self._kv.get(keys) for key in keys]

    def mset(self, *items):
        data = zip(items[::2], items[1::2])
        for key, value in data:
            self._kv[key] = value
        return 1

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FlUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset
        }

    def connection_handler(self, conn, address):
        socket_file = conn.makefile('rwb')

        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                resp = Error(exc.args[0])
            self._protocol.write_response(socket_file, resp)

    def get_response(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be list or simple string')

        if not data:
            raise CommandError('Missing Command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError(f'Unrecognized command : {command}')


        return self._commands[command](*data[1:])

    def run(self):
        self._server.serve_forever()


if __name__ == '__main__':
    from gevent import monkey; monkey.patch_all()
    Server().run()