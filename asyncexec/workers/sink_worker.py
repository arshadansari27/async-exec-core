from . import Actor


class SinkWorker(object):

    def __init__(self, loop, pool, func, publisher, ready_event, terminate_event):
        self.loop = loop
        self.actor = Actor(pool, func, is_generator=False)
        self.client = loop.run_until_complete(self.actor.start())
        self.publisher = publisher
        self.event = None
        self.ready_event = ready_event
        self.terminate_event = terminate_event

    async def start(self):
        count = 0
        self.ready_event.data = 'SinkWorker'
        self.ready_event.set()
        while True:
            try:
                if self.publisher.empty() and self.terminate_event.is_set():
                    break
                count += 1
                data = await self.publisher.publish()
                await self.client.call.handler(data)
            except Exception as e:
                print('Error occured', e)
                raise
        if not self.terminate_event.is_set():
            self.terminate_event.data = 'DONE'
            self.terminate_event.set()
