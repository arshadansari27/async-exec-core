import sys
import asyncio
import simplejson as json
from aiokafka import AIOKafkaProducer
import traceback


async def main(loop, ip, port, queue, message):
    kproducer = None
    try:
        kproducer = AIOKafkaProducer(loop=loop, bootstrap_servers=str(ip) + ":" + str(port))
        await kproducer.start()
        for i in range(10000):
            message_body = message.encode('utf-8')
            _ = await kproducer.send_and_wait(queue, message_body)
    except Exception as e:
        traceback.print_exc()
        exit(1)
    finally:
        if kproducer: kproducer.stop()


if __name__ == "__main__":
    ip = sys.argv[1]
    port = 9092
    queue = sys.argv[2]
    message = sys.argv[3]
    print ("senging to queue", queue, 'message:', message)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, ip, port, queue, message))
