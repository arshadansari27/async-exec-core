import pytest
import math
import os
import unittest
import sys
sys.path.insert(0, os.getcwd())
import models_test
from commands import Command
from commands.unit import UnitCommand
from commands.composite import SequentialCommandGroup, ParallelCommandGroup

def method_to_run(x):
    return x * 2

def method_to_callback(rs):
    return sum(rs)


class TestSequentialCommandGroup(unittest.TestCase):

    def setUp(self):
        self.commands = []
        for i in range(1, 4):
            command = method_to_run
            self.commands.append(command)
        print("Length", len(self.commands))

    def test_execute(self):
        command = SequentialCommandGroup(1, self.commands)
        self.assertEquals(16, command.execute(2))

    def tearDown(self):
        pass

class TestParallelCommandGroup(unittest.TestCase):

    def setUp(self):
        self.commands = []
        for i in range(1, 4):
            command = method_to_run
            self.commands.append(command)
        print("Length", len(self.commands))
        self.callback = method_to_callback

    def test_execute(self):
        command = ParallelCommandGroup(1, self.commands, self.callback)
        self.assertEquals(12, command.execute(2))

    def test_execute_without_callback(self):
        command = ParallelCommandGroup(1, self.commands)
        self.assertEquals(3, len(command.execute(2)))

    def test_execute_threaded(self):
        command = ParallelCommandGroup(1, self.commands, self.callback)
        self.assertEquals(12, command.execute(2, sync=False))

    def test_execute_multiprocess(self):
        command = ParallelCommandGroup(1, self.commands, self.callback)
        self.assertEquals(12, command.execute(2, sync=False, multiprocessing=True))



    def tearDown(self):
        pass


class TestMixedCommandGroup(unittest.TestCase):

    def setUp(self):
        pass

    def test_execute(self):
        self.commands = []
        for i in range(1, 4):
            command = method_to_run
            self.commands.append(command)
        self.callback = method_to_callback
        command1 = ParallelCommandGroup(1, self.commands, self.callback)
        command2 = ParallelCommandGroup(1, self.commands, self.callback)
        command = SequentialCommandGroup(1, [command1, command2])

        self.assertEquals(72, command.execute(2))

    def tearDown(self):
        pass
