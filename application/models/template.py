from . import Model


class Template(Model):

       def __init__(self, id, name=None, location=None, code=None,
                    version=None, createdAt=None, updatedAt=None):
              self.id = id
              self.name = name
              self.location = location
              self.code = code
              self.createdAt = createdAt
              self.updatedAt = updatedAt
              self.version = version

       def updateCode(version, code):
              if version > self.version
                     self.version = version
                     self.code = code
