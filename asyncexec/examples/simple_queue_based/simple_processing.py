import sys
sys.path.insert(0, '../../../')

from asyncexec.exec import  AsyncExecutor


async_executor = AsyncExecutor({
    "redis": {
        "host": "localhost",
        "port": 6379
    },
    "rabbitmq": {
        "host": "localhost",
        "port": '',
        "username": "guest",
        "password": "guest"
    }
})

@async_executor.handler('rabbitmq', 'req_queue1', 'res_queue1')
def test_method1(data):
    print("Hello", data)
    return 'result'

@async_executor.handler('redis', 'req_queue2', 'res_queue2')
def test_method2(data):
    print("Bye", data)
    return str(data) + ': response'


async_executor.start()
