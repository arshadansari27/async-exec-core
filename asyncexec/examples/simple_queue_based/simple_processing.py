import sys
sys.path.insert(0, '../../../')

from asyncexec.exec import  AsyncExecutor
'''
    "redis": {
        "host": "172.17.0.3",
        "port": 6379
    }
    "rabbitmq": {
        "host": "172.17.0.2",
        "port": '',
        "username": "user",
        "password": "password"
    },
'''

async_executor = AsyncExecutor({
    'process_info': {
        'workers': 10,
        'pool': 'process'
    },
    "rabbitmq": {
        "host": "172.17.0.3",
        "port": 5672,
        "username": "guest",
        "password": "guest"
    }
})

import time
@async_executor.handler('rabbitmq', 'rabbit:in_q', 'rabbit:out_q')
def test_method(data):
    return 'result'

async_executor.start()
