import simplejson as json
from concurrent.futures import ProcessPoolExecutor
from uuid import uuid4
        
process_pool_executor = ProcessPoolExecutor(max_workers=3)

class Handler(object):

    def __init__(self, key, callback):
        if not callable(callback):
            raise Exception('callback is not callable')
        self.key = key
        self.callback = callback

    def handle(self, data):
        print ("[*] Calling handler")
        return self.callback(data)

async def run_in_executor_and_response_async(handler, event):
    return run_in_executor_and_response(handler, event)

def run_in_executor_and_response(handler, event):
    result = handler.handle(event)
    response = {'id': event['id'], 'response': result}
    print("[*] Response: ", response, '\n')
    return json.dumps(response)

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

    async def handle(self, event, loop, queue_req, queue_resp=None, multiprocess=True, callback=None):
        event = json.loads(event)
        if not event.get('id'):
            event['id'] = str(uuid4())
        else:
            event['id'] = str(event['id'])
        if queue_req not in self.subscribers:
            print("[*] No handler is registered for this event")
            return None
        event_handler = self.subscribers[queue_req]
        if multiprocess:
            response = await loop.run_in_executor(process_pool_executor, 
                             run_in_executor_and_response, event_handler, event)
        else:
            response = await loop.create_task(run_in_executor_and_response_async(event_handler, event))
        if queue_resp:
            await callback(queue_resp, response)
        else:
            print("No response to be generated")
            return None
        return response


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
