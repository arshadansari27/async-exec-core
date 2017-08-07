(asyncexec) Backend Asynchronous Task and Workflows Execution 
=============================================================

A framework/library to make working with asynchronous backend process with
complex workfows an easy experience for those not interested in investing too
much to the projects infrastructure.
For those who have experience with celery will find that this project is very
similar in intent but much smaller in scope. The latest version of the project
allows defining backend tasks in python and running them from any other system
by pushing messages on middleware and letting the framework do the rest. User
can either listen on a seperate response queue for results or may completely
ignore them. 

Currently, support is present for rabbitmq (Direct exchange) and redis (List -
push / pop) and HTTP (POST/GET). Redis pub/sub is not supported since it does only supports
broadcast messaging to all subscribers instead of one - to - one mapping, which
causes the problem with multiple workers listening on the same queue. This
project is best fit for new start ups working with working knowledge of python,
to setup daemon services and worry only about the business logic instead of
setting up the infrastructure code in their project.

NOTE: HTTP listener is for debugging purpose and can also be used to handle
requests and run the corresponding handler by giving the name of the queue as
the path and body as json of the requests /<queue>.


License:
--------
MIT License


Requirements: 
-------------

* Python 3.5+, 
* rabbitmq or redis

Installation: 
-------------
pip install git+https://github.com/arshadansari27/async-exec-core.git



How it works
------------

Check example at asyncexec/examples folder for simple backend processing as well
as for making complex workflows using parallel and sequential command groups.


Release notes:
--------------

* v0.1.0 Tag contains
    - Based on asyncio module of python 3.6
    - RabbitMQ support
    - Redis Support
    - HTTP support
    - Workflows creation using ParallelCommandGroup and SequentialCommandGroup
    - Simple annotation based addition of handlers (daemon service) like in the
      case of celery or flask
    - Queue based handler definition association through decorators. Check examples
    - Multiprocessing support for individual handlers
    - Workflows supporting sequential and parallel command groups with their combinations
    - Multiprocessing support with ParallelCommandGroup

Micro Benchmark Results
-----------------------

Machine details:
* Ubuntu 17.04
* Intel i5, 2 physical cores and in total 4 virtual cores
* 16 GB RAM
* Caveats:
	- Redis and RabbitMQ running on a docker on the same machine
	- The publisher and the subscriber were also running on the same machine
	- The Publisher was pushing messages sequentially
	- Multiprocessing had 3 workers

|Task Details | Rabbit MQ (multiprocessing enabled) | Rabbit MQ (without multiprocessing enabled) | Redis (multiprocessing enabled) | Redis (without multiprocessing enabled) |
| --- | --- | --- | --- | --- |
| Trivial (do nothing) |  ~ 450 req/sec | ~ 750 req/sec | ~ 900 req/sec | ~ 1600 req/sec |
| time.sleep(.1) | ~ 30 req/sec | ~ 10 req/sec | ~ 29 req/sec | ~ 8 req/sec |
| time.sleep(1) | ~ 3.5 req/sec | ~ 1.5 req/sec | ~ 2.8 req/sec | ~ 1 req/sec |



Used by
-------
* [Stockopedia](http://assets.stockopedia.com/redesign/logo.png)


Contribution and Other information:
-----------------------------------
Please check the wiki pages for more information and about how to contribute.


