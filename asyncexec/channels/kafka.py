import aioredis
from asyncexec.channels import Listener, Publisher
import traceback
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logger = logging.getLogger(__name__)

class KafkaListener(Listener):

    def __init__(self, loop, configurations, queue_name, consumer, start_event, terminate_event, flow_id=None):
        super(KafkaListener, self).__init__(loop, configurations, queue_name, consumer, start_event, terminate_event, flow_id=flow_id)

    async def start(self):
        kconsumer = None
        try:
            kconsumer = AIOKafkaConsumer(self.queue_name, loop=self.loop, bootstrap_servers=str(self.host) + ":" + str(self.port), group_id="async_group")
            await self.start_event.wait()
            if self.terminate_event.is_set():
                raise Exception("Kafka Listener: Termination event occurred before starting...")
            await kconsumer.start()
            async for message in kconsumer:
                _msg = message.value.decode('utf-8')
                print('->', _msg)
                await self.consumer.consume(_msg)
        except Exception as e:
            logger.error(e)
            traceback.print_exc()
            exit(1)
        finally:
            if kconsumer: kconsumer.stop()


class KafkaPublisher(Publisher):

    def __init__(self, loop, configurations, queue_name, publisher, ready_event, terminate_event, flow_id=None):
        super(KafkaPublisher, self).__init__(loop, configurations, queue_name, publisher, ready_event, terminate_event, flow_id=flow_id)

    async def start(self):
        kproducer = None
        try:
            kproducer = AIOKafkaProducer(loop=self.loop, bootstrap_servers=str(self.host) + ":" + str(self.port))
            await kproducer.start()
            self.ready_event.data = 'KafkaPublisher'
            self.ready_event.set()
            logger.info('[Kafka: {}](Publisher) started...'.format(self.flow_id))
            while True:
                if  self.publisher.empty() and self.terminate_event.is_set():
                    break
                message = await self.publisher.publish()
                _ = await kproducer.send_and_wait(self.queue_name, str(message).encode('utf-8'))
        except Exception as e:
            logger.error(e)
            traceback.print_exc()
            exit(1)
        finally:
            if kproducer: kproducer.stop()
