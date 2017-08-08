from . import Actor


class InOutWorker(object):

    def __init__(self, loop, pool, func, publisher, consumer):
        self.loop = loop
        self.actor = Actor(pool, func, is_generator=True)
        self.client = loop.run_until_complete(self.actor.start())
        self.publisher = publisher
        self.consumer = consumer

    async def start(self):
        while True:
            data  = await self.publisher.publish()
            response = await self.client.call.handler(data)
            await self.consumer.consume(response)
