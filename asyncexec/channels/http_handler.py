import asyncio
from datetime import datetime
from aiohttp import web
import random
import simplejson as json

async def create_http_listener(loop, listener, port):
    async def handler(request):
        name = [u for u in request.raw_path.split('/') if len(u) > 0][0]
        if request.method != 'POST':
            return web.Response(text='Method not supported', status=405)
        data = await request.text()
        if not data:
            return web.Response(text='Invalid Data', status=400)
        print('[*]', data)
        await listener.handle(data, loop, name, None, None)
        return web.Response(text='ok')

    server = web.Server(handler)
    await loop.create_server(server, "0.0.0.0", port)

