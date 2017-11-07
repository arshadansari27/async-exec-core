import asyncio
from datetime import datetime
from aiokafka import AIOKafkaConsumer
import sys, traceback

loop = asyncio.get_event_loop()

first_message = False
arrival_begin_time_stamp = None
message_count = 0


def on_message(message):
    global first_message, message_count, arrival_begin_time_stamp
    with message.process():
        '''
        if not first_message:
            first_message = True
            arrival_begin_time_stamp = datetime.now()
        d = datetime.now() - arrival_begin_time_stamp
        dd = d.days
        ds = d.seconds
        diff = (3600 * 24) * dd + ds 
        '''
        message_count += 1
        print("[x] %s %d" % (message.body, message_count))


async def main(loop, ip, port, queue):
    kconsumer = None
    try:
        kconsumer = AIOKafkaConsumer(queue, loop=loop, bootstrap_servers=str(ip) + ":" + str(port), group_id="async_group")
        await kconsumer.start()
        async for message in kconsumer:
            print(message)
    except Exception as e:
        traceback.print_exc()
        exit(1)
    finally:
        if kconsumer: kconsumer.stop()


if __name__ == "__main__":
    ip = sys.argv[1]
    port = 9092
    queue = sys.argv[2]
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop, ip, port, queue))
    print(' [*] Waiting for logs. To exit press CTRL+C. Listening on', ip, queue)
    loop.run_forever()
