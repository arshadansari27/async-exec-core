import abc

class Listener(object):

	def __init__(self):
		self.subscribers = set([])

	def subscribe(self, subscriber):
		if subscriber in self.subscribers:
			return
		self.subscribers.add(subscriber)

	def unsubscribe(self, subscriber):
		if subscriber not in self.subscribers:
			return
		self.subscribers.remove(subscriber)

	def handle_event(self, event):
		for subscriber in self.subscribers:
			if subscriber.observes(event):
				subscriber.handle(event)


class Subscriber(metaclass=abc.ABCMeta):

	@abc.abstractmethod
	def observes(self, event):
		pass

	@abc.abstractmethod
	def handle(self, event):
		pass
