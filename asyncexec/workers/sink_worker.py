from . import Actor
import traceback


class SinkWorker(object):

    def __init__(self, loop, pool, func, publisher, ready_event, terminate_event, callback=None, count=None):
        self.loop = loop
        self.actor = Actor(pool, func, is_generator=True)
        self.client = loop.run_until_complete(self.actor.start())
        self.publisher = publisher
        self.event = None
        self.ready_event = ready_event
        self.terminate_event = terminate_event
        if callback:
            assert callable(callback)
            self.callback = callback
        self.count = count

    async def start(self):
        count = 0
        self.ready_event.data = 'SinkWorker'
        self.ready_event.set()
        result = None
        while True:
            try:
                count += 1
                if self.publisher.empty() and self.terminate_event.is_set():
                    break
                for data in await self.publisher.publish()
                    result = await self.client.call.handler(result, data)
                    if self.count and self.callback and (count % self.count is 0):
                        self.callback(result)

            except Exception as e:
                traceback.print_exc()
                exit(1)
        if self.callback:
            self.callback(result)
        if not self.terminate_event.is_set():
            self.terminate_event.data = 'DONE'
            self.terminate_event.set()
        print('Sink worker done..')
