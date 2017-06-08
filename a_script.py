from asyncexec.exec import  AsyncExecutor


async_executor = AsyncExecutor({
    "redis": [
        ['pyasync_core_request', 'pyasync_core_result']
    ]
})


@async_executor.handler
def test_method(data):
    print("Hello", data)


async_executor.start()
