import asyncio
from datetime import datetime
from aiohttp import web
import aiohttp
import random
import simplejson as json
from asyncexec.channels import Listener, Publisher, External


class HTTPWorker(External):
    def __init__(self, loop, configurations, endpoint, publisher, consumer):
        super(HTTPWorker, self).__init__(loop, configurations, endpoint)
        self.publisher = publisher
        self.consumer = consumer

    async def start(self):
        pass



class HTTPPublisher(Publisher):

    def __init__(self, loop, configurations, endpoint, publisher):
        super(HTTPPublisher, self).__init__(loop, configurations, endpoint)
        self.publisher = publisher

    async def start(self):
        async with aiohttp.ClientSession() as session:
            raw_data = await self.publisher.publish()
            async with session.post(self.queue_name, data=raw_data) as response:
                if response.status != 200:
                    raise Exception("Something went wrong when making a call to ", self.queue_name)
                response_data = await response.text()
                print(response_data)


class HTTPListener(Listener):

    server = None
    handlers = {}

    def __init__(self, loop, configurations, endpoint, consumer):
        super(HTTPListener, self).__init__(loop, configurations, endpoint, consumer)
        if not self.__class__.server:
            host, port = self.host, int(self.port)
            print('Starting http', host, port, self.queue_name)
            self.__class__.server = self.loop.run_until_complete(
                self.loop.create_server(web.Server(HTTPListener.handler_proxy), host, port))

    @classmethod
    async def handler_proxy(cls, request):
        # name = [u for u in request.raw_path.split('/') if len(u) > 0][0]
        print('Recv', request.raw_path)
        handler = cls.handlers.get(request.raw_path)
        return await handler(request)

    async def start(self):
            self.__class__.handlers[self.queue_name] = self.handler

    async def handler(self, request):
        if request.method != 'POST':
            return web.Response(text='Method not supported', status=405)
        data = await request.text()
        if not data:
            return web.Response(text='Invalid Data', status=400)
        await self.consumer.consume(json.dumps(data).encode('utf-8'))
        return web.Response(text='ok')


if __name__ == '__main__':
    h = HTTPWorker(None, {
        'host': 'host',
        'port': 'port',
        'username': 'username',
        'password': 'password'
    }, 'endpoint', 'publisher', 'comsumer')
    print(h.host)
    print(h.port)
    print(h.username)
    print(h.password)
    print(h.queue_name)
    print(h.publisher)
    print(h.consumer)
