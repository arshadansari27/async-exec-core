Backend Asynchronous Task Execution
===================================

In other words, celery simplified. Has support for direct messaging in rabbitmq,
pubsub with redis and future work includes plan for using zmq. Problem with
redis pubsub method is that it is not scalable for direct messaging, only
broadcast messages are scalable.

Requirements: Python 3.5+, rabbitmq or redis

How it works
------------

Client program to send message via redis or rabbitmq is required (currently
under development). A seperate process can run as shown in example which will
listen on the messages from the queues and based on the content of message will
select the correct handler to run with the date from the message.

Check example at asyncexec/examples folder for simple backend processing as well
as for making complex workflows using parallel and sequential command groups.


ROAD MAP:
---------
    * Handlers specific to queue names
    * Redis PubSub with direct messaging instead of broadcast
    * ZMQ support


