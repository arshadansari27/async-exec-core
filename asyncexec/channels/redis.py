import aioredis
from asyncexec.channels import Listener, Publisher
import traceback

class RedisListener(Listener):

    def __init__(self, loop, configurations, queue_name, consumer, start_event, terminate_event, flow_id=None):
        super(RedisListener, self).__init__(loop, configurations, queue_name, consumer, start_event, terminate_event, flow_id=flow_id)

    async def start(self):
        conn_pool = None
        try:
            conn_pool = await aioredis.create_redis((self.host, self.port))
            await self.start_event.wait()
            if self.terminate_event.is_set():
                raise Exception("REDIS Listener: Termination event occurred before starting...")
            while True:
                while await conn_pool.llen(self.queue_name) > 0:
                    _, message = await conn_pool.blpop(self.queue_name)
                    await self.consumer.consume(message)
        except Exception as e:
            traceback.print_exc()
            exit(1)
        finally:
            if conn_pool:
                conn_pool.close()


class RedisPublisher(Publisher):

    def __init__(self, loop, configurations, queue_name, publisher, ready_event, terminate_event, flow_id=None):
        super(RedisPublisher, self).__init__(loop, configurations, queue_name, publisher, ready_event, terminate_event, flow_id=flow_id)

    async def start(self):
        conn_pool = None
        try:
            conn_pool = await aioredis.create_redis((self.host, self.port))
            self.ready_event.data = 'RedisPublisher'
            self.ready_event.set()
            print('[Redis: {}](Publisher) started...'.format(self.flow_id))
            while True:
                if  self.publisher.empty() and self.terminate_event.is_set():
                    break
                message = await self.publisher.publish()
                _ = await conn_pool.lpush(self.queue_name, message)
        except Exception as e:
            traceback.print_exc()
            exit(1)
        finally:
            if conn_pool:
                conn_pool.close()
