import aioredis
from asyncexec.channels import Listener, Publisher


class RedisListener(Listener):

    def __init__(self, loop, configurations, queue_name, consumer):
        super(RedisListener, self).__init__(loop, configurations, queue_name, consumer)

    async def start(self):
        conn_pool = await aioredis.create_redis((self.host, self.port))
        try:
            while True:
                while await conn_pool.llen(self.queue_name) > 0:
                    _, message = await conn_pool.blpop(self.queue_name)
                    await self.consumer.consume(message)
        except:
            raise
        finally:
            if conn_pool:
                conn_pool.close()


class RedisPublisher(Publisher):

    def __init__(self, loop, configurations, queue_name, publisher):
        super(RedisPublisher, self).__init__(loop, configurations, queue_name, publisher)

    async def start(self):
        conn_pool = await aioredis.create_redis((self.host, self.port))
        try:
            while True:
                message = await self.publisher.publish()
                _ = await conn_pool.lpush(self.queue_name, message)
        except:
            raise
        finally:
            if conn_pool:
                conn_pool.close()
