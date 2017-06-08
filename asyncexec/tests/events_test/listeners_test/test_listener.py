import pytest
import math
import os
import unittest
import sys
sys.path.insert(0, os.getcwd())
import models_test
from events.listeners import Listener, Subscriber

class ExampleEvent(object):

       def __init__(self, id):
              self.id = id

       def __repr__(self):
              return str(self.id)


class SubscriberImplementation(Subscriber):

       def __init__(self, id):
              self.id = id

       def observes(self, event):
              if self.id % 2 is 0:
                     return event.id % 2 is 0
              else:
                     return event.id % 2 is not 0

       def handle(self, event):
              print("Subscriber {} handling event {}".format(str(self.id), str(event)))

       def __repr__(self):
              return str(self.id)

       def __hash__(self):
              return self.id.__hash__()

       def __eq__(self, other):
              if not isinstance(other, Subscriber):
                     return False
              return self.id == other.id

class TestListenerSubscription(unittest.TestCase):

       def setUp(self):
              self.listener = Listener()


       def test_subscribe_unsubscribe(self):
              for i in range(10):
                     self.listener.subscribe(SubscriberImplementation(i % 5))
              self.assertEquals(len(self.listener.subscribers) , 5)
              for i in range(10):
                     self.listener.unsubscribe(SubscriberImplementation(i % 5))
              self.assertEquals(len(self.listener.subscribers) , 0)

       def tearDown(self):
              pass

class TestListenerNotification(unittest.TestCase):

       def setUp(self):
              self.listener = Listener()
              for i in range(10):
                     self.listener.subscribe(SubscriberImplementation(i % 5))


       def test_subscribe_unsubscribe(self):
              for i in range(10):
                     event = ExampleEvent(i)
                     self.listener.handle_event(event)

       def tearDown(self):
              pass
