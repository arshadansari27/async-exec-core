import pytest
import math
import os
import unittest
import sys
sys.path.insert(0, os.getcwd())
import models_test
from commands import Command
from commands.unit import UnitCommand
from commands.composite import SequentialCommandGroup


class FakeCommand(Command):

       def __init__(self):
              pass

       def execute(self, context):
              context['x'] = context['x'] * 2
              return context


class TestSequentialCommandGroup(unittest.TestCase):

       def setUp(self):
              self.commands = []
              for i in range(1, 4):
                     command = FakeCommand()
                     self.commands.append(command)
              print("Length", len(self.commands))

       def test_execute(self):

              command = SequentialCommandGroup(1, {}, self.commands)
              self.assertEquals(16, command.execute({'x': 2}).get('x'))

       def tearDown(self):
              pass
