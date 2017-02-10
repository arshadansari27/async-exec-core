

class CommandBegun(object):

	def __init__(self, command, timestamp):
		self. command = command
		self.timestamp = timestamp


class CommandCompleted(object):

	def __init__(self, command, timestamp):
		self. command = command
		self.timestamp = timestamp

class CommandFailed(object):

	def __init__(self, command, timestamp):
		self. command = command
		self.timestamp = timestamp
