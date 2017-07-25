import asyncio
import aiozmq
import aiozmq.rpc
import sys, traceback


class Channel(object):

    def __init__(self, uri, handler_key, sync=True):
        self.uri = uri
        self.handler_key = handler_key
        self.sync = sync

    async def initialize(self):
        print("Channel to zmq server address", self.__class__.__name__, self.uri)
        self.client = await aiozmq.rpc.connect_rpc(connect=self.uri)

    async def call(self, *kargs, **kwargs):
        print("In channel call")
        try:
            rs = await getattr(self.client.call, self.handler_key)(*kargs, **kwargs)
            if self.sync:
                self.write_response(rs)
            return rs
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            raise 

    def set_writer(self, writer):
        print("Setting writer", writer)
        self.writer = writer

    def write_response(self, result):
        print("[*] Writing response", result)
        if self.writer:
            self.writer.write_response(result)

    def close(self):
        self.client.close()


if __name__ == '__main__':

    @aiozmq.rpc.method
    def test_handler(x, y):
        return x + y

    handlers = {
        'test_handler': test_handler
    }

    uri = 'tcp://127.0.0.1:5555'

    async def start_server(handlers):
        command = await aiozmq.rpc.serve_rpc(handlers, bind=uri)
        command_addr = list(command.transport.bindings())[0]
        return command

    async def stop_server(command):
        command.close()

    async def run_client():
        channel = Channel(uri, 'test_handler', sync=True)
        await channel.initialize()
        response = await channel.call(10, 20)
        assert response == 30

    loop = asyncio.get_event_loop()
    command = loop.run_until_complete(start_server(handlers))
    loop.run_until_complete(run_client())
    loop.run_until_complete(stop_server(command))
