from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
from collections import namedtuple
from io import BytesIO
from socket import error as socket_error


class CommandError(Exception): pass


class Disconnect(Exception): pass


Error = namedtuple('Error', ('message',))


class ProtocolHandler(object):
    def handle_request(self, socket_file):
        pass

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