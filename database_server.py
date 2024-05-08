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
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_integer,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dict}
    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect
        try:
            return self.handlers[first_byte](socket_file)
        except KeyError :
            raise CommandError('bad request')

    def handle_simple_string(self,socket_file):
        return socket_file.readline().rstrip('\r\n')

    def handle_error(self,socket_file):
        return Error(socket_file.readline().rstrip('\r\n'))

    def handle_integer(self,socket_file):
        return int(socket_file.readline().rstrip('\r\n'))

    def handle_string(self,socket_file):
        length = int(socket_file.readline().rstrip('\r\n'))
        if length == -1:
            return None
        length +=2
        return socket_file.read(length)[:-2]

    def handle_array(self,socket_file):
        num_elements=int(socket_file.readline().rstrip('\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self,socket_file):
        num_items=int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range (num_items*2)]
        return dict(zip(elements[::2],elements[1::2]))

    def write_response(self, socket_file, data):
        pass


class Server(object):
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host,port),
            self.connection_handler,
            spawn=self.pool
        )
        self._protocol = ProtocolHandler()
        self._kv = {}

    def connection_handler(self,conn,address):
        socket_file = conn.makefile('rwb')

        while True:
            try:
                data= self._protocol.handle_request(socket_file)
            except Disconnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                resp = Error(exc.args[0])

    def get_response(self):
        pass

    def run(self):
        self._server.serve_forever()