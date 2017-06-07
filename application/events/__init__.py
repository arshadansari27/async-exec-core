class Handler(object):

    def __init__(self, key, callback):
        if not callable(callback):
            raise Exception('callback is not callable')
        self.key = key
        self.callback = callback

    async def handle(self, data):
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

    def handle(self, event):
        if event['event'] not in self.subscribers:
            print("[*] No handler is registered for this event")
            return None
        data = event['data']
        print ("[*] Handling event", event['event'], 'with data', data)
        return self.subscribers[event['event']].handle(data)

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
