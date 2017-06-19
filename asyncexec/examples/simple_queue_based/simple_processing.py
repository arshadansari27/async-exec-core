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
    "rabbitmq": {
        "host": "172.17.0.2",
        "port": '',
        "username": "user",
        "password": "password"
    }
})
import time
@async_executor.handler('rabbitmq', 'req_queue1', 'res_queue1', multiprocess=True)
def test_method1(data):
    return 'result'

'''
@async_executor.handler('rabbitmq', 'req_queue2', 'res_queue2', multiprocess=False)
def test_method2(data):
    print("Bye", data)
    return str(data) + ': response'
'''

async_executor.start()
