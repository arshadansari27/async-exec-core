class Model(object):

       def __init__(self, id, **data):
              self.id = id
              for k, v in data.items():
                     setattr(self, k, v)
