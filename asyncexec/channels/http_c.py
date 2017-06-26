import asyncio
from datetime import datetime
from aiohttp import web
import random
import simplejson as json
from asyncexec.core.channels import Channel


class HTTPChannel:

    initialized = False
    channel_type = Channel

    def __init__(self, handler_uris, handler_key):
        self.handler_uris = handler_uris
        self.zmq_channels = []
        for handler_uri in self.handler_uris:
            self.zmq_channels.append(
                HTTPChannel.channel_type(handler_uri, handler_key))

    async def start(self):
        cls = self.__class__

        for zmq_channel in self.zmq_channels:
            await zmq_channel.initialize()
            zmq_channel.set_writer(self)

        self.server = web.Server(self.handler)
        print("starting http...")
        await self.loop.create_server(self.server, "0.0.0.0", self.__class__.port)
        print("Waiting to consume on http...")

    def write_response(self, result):
        print("Writing Response", result)

    @classmethod
    async def initialize(cls, config, loop):
        # Use for basic auth?
        cls.username =  config['username'] 
        cls.password =  config['password'] 
        cls.host = config['host'] 
        cls.port = config['port']
        cls.initialized = True
        cls.loop = loop

    async def handler(self, request):
        name = [u for u in request.raw_path.split('/') if len(u) > 0][0]
        if request.method != 'POST':
            return web.Response(text='Method not supported', status=405)
        data = await request.text()
        if not data:
            return web.Response(text='Invalid Data', status=400)
        selector = random.randint(0, len(self.zmq_channels) - 1)
        await self.zmq_channels[selector].call(data)
        return web.Response(text='ok')

if __name__ == '__main__':
    host = '0.0.0.0'
    port =  8888
    username =  ''
    password =  ''

    class DummyChannel:
        def __init__(self, uri, handler_key, sync=True):
            self.sync = sync

        async def initialize(self):
            pass

        async def call(self, *kargs, **kwargs):
            print("Channel handler called")
            if self.sync:
                self.write_response(b"Writing fake response")
            return b"Writing fake response"
    
        def set_writer(self, writer):
            self.writer = writer

        def write_response(self, result):
            if self.writer:
                self.writer.write_response(result)

        def close(self):
            pass

    HTTPChannel.channel_type = DummyChannel
    handler_uris = ['DUMMY HANDLER']
    loop = asyncio.get_event_loop()
    loop.run_until_complete(HTTPChannel.initialize({'host': host, 'port': port, 'username': username, 'password': password}, loop))
    handler_key = 'test_handler'
    http_channel = HTTPChannel(handler_uris, handler_key)
    loop.create_task(http_channel.start())
    loop.run_forever()
