import aioredis
from asyncexec.channels import Listener, Publisher
import traceback
import logging
logger = logging.getLogger(__name__)

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
            logger.error(e)
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
            logger.info('[Redis: {}](Publisher) started...'.format(self.flow_id))
            while True:
                if  self.publisher.empty() and self.terminate_event.is_set():
                    break
                message = await self.publisher.publish()
                if hasattr(message, '__iter__'):
                    for _message in await self.publisher.publish():
                        if _message == 'FAKE_SIGNALLING_DATA_TO_TERMINATE':
                            logger.info("Received signal to terminate process")
                            continue

                        _ = await conn_pool.lpush(self.queue_name, _message)
                else:
                    if message != 'FAKE_SIGNALLING_DATA_TO_TERMINATE':
                        _ = await conn_pool.lpush(self.queue_name, message)
                    else:
                        logger.info("Received signal to terminate process")
        except Exception as e:
            logger.error(e)
            traceback.print_exc()
            exit(1)
        finally:
            if conn_pool:
                conn_pool.close()
