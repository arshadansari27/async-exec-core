import simplejson as json
from uuid import uuid4

class Handler(object):

    def __init__(self, key, callback):
        if not callable(callback):
            raise Exception('callback is not callable')
        self.key = key
        self.callback = callback

    async def handle(self, data):
        print ("[*] Calling handler")
        return self.callback(data)


class Listener(object):

    def __init__(self):
        self.subscribers = {}

    def subscribe(self, subscriber):
        if subscriber in self.subscribers:
            return
        self.subscribers[subscriber.key] = subscriber

    def unsubscribe(self, subscriber_key):
        if subscriber_key not in self.subscribers:
            return
        del self.subscribers[subsciber_key]

    async def handle(self, event, loop, queue_req, queue_resp, callback):
        event = json.loads(event)
        if not event.get('id'):
            event['id'] = str(uuid4())
        else:
            event['id'] = str(event['id'])
        if queue_req not in self.subscribers:
            print("[*] No handler is registered for this event")
            return None
        result = self.subscribers[queue_req].handle(event)
        if queue_resp is None:
            return None
        response = {'id': event['id'], 'response': (await result)}
        print("[*] Response: ", response, '\n')
        loop.create_task(callback(queue_resp, json.dumps(response)))

    @staticmethod
    def create_listener():
        listener = Listener()
        return listener

    def register_handler(self, handler):
        self.subscribe(handler)

    def unregister_handler(self, handler):
        if isinstance(handler, Handler):
            key = handler.key
        else:
            key = handler
        self.unsubscribe(handler)
