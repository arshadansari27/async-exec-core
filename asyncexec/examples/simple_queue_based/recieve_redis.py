import asyncio
import aioredis
from functools import partial
import sys
import simplejson as json
from datetime import datetime

first_message = False
arrival_begin_time_stamp = None
message_count = 0



async def main(loop, ip, queue):
    global first_message, message_count, arrival_begin_time_stamp
    # Perform connection
    pool = await aioredis.create_pool((ip, 6379), minsize=5, maxsize=20)

    with (await pool) as redis:

        while True:
            if not first_message:
                first_message = True
                arrival_begin_time_stamp = datetime.now()
            _, event = await redis.blpop(queue)
            d = datetime.now() - arrival_begin_time_stamp
            dd = d.days
            ds = d.seconds
            diff = (3600 * 24) * dd + ds 
            message_count += 1.
            print("[x] %r" % event, message_count / diff if diff > 0 else message_count)
    print ("closing")
    pool.close()

if __name__ == "__main__":
    ip = sys.argv[1]
    queue = sys.argv[2]
    loop = asyncio.get_event_loop()
    print(' [*] Waiting for logs. To exit press CTRL+C. Listening on', ip, queue)
    loop.run_until_complete(main(loop, ip, queue))
