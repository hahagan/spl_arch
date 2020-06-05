# -*- coding: utf-8 -*-
"""
    File Name: executor
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 14:04
"""
from spl_arch.input.spl_input import SplInput
from spl_arch.parser.antlr_parser import AntlrParser
from spl_arch.scheduler.scheduler import Scheduler


class Executor(object):
    """
        spl bootstrap
    """

    def __init__(self, name, start_time, end_time):
        self.name = name
        self.start_time = start_time
        self.end_time = end_time

    def time_consume(self):
        pass

    def execute(self):
        """
        executer entrance

        1. input
        2. parse
        3. scheduler --> search (extract)
        4. ...
        :return: void
        """
        # input module
        spl_cmd = SplInput().get_input()  # search repo="mytest"

        # parse module
        # input -> pipe_cmd
        # output -> collection of opts
        antlr_parser = AntlrParser("antlr", spl_cmd)
        antlr_parser.validate()
        opts = antlr_parser.parse()

        # schedule module
        scheduler = Scheduler("scheduler")
        scheduler.schedule(opts)


if __name__ == "__main__":
    executor = Executor("spl", 0, 0)
    executor.execute()
