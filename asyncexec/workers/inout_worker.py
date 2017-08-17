from . import Actor


class InOutWorker(object):

    def __init__(self, loop, pool, func, publisher, consumer, ready_event, terminate_event):
        self.loop = loop
        self.actor = Actor(pool, func, is_generator=True)
        self.client = loop.run_until_complete(self.actor.start())
        self.publisher = publisher
        self.consumer = consumer
        self.ready_event = ready_event
        self.terminate_event = terminate_event

    async def start(self):
        self.ready_event.data = 'InOutWorker'
        self.ready_event.set()
        while True:
            if self.publisher.empty() and self.terminate_event.is_set():
                break
            data = await self.publisher.publish()
            response = await self.client.call.handler(data)
            await self.consumer.consume(response)
        if not self.terminate_event.is_set():
            self.terminate_event.data = 'DONE'
            self.terminate_event.set()
