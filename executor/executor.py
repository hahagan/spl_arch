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
from spl_arch.stream.es_stream import EsStream
from spl_arch.stream.local_mem_stream import LocalMemoryQueue


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


    @staticmethod
    def parallel_execute():
        """
        executer entrance

        1. input
        2. parse
        3. scheduler --> search (extract)
        4. ...
        :return: void
        """
        # input module
        # spl_cmd = SplInput().get_input()  # search repo="mytest"
        spl_cmd = '"search indexer="hello" | replace "hello" with "world" in class | stats avg(math) as avg_math by class"'

        # parse module
        # input -> pipe_cmd
        # output -> collection of opts
        antlr_parser = AntlrParser("antlr", spl_cmd)
        antlr_parser.validate()
        opts = antlr_parser.parse()

        # CommandException and it’s lock, CommandException can get command error info
        from spl_arch.scheduler.CommandException import CommandException
        from threading import Lock
        lock = Lock()
        exception = CommandException(lock)

        # combine Stream between cmd
        def stream_builder(cmd_opts, ex_lock, ex):
            # 一种将命令与数据流串联算法
            input_stream = None
            end_stream = None
            for _index, cmd_opt in enumerate(cmd_opts):
                if _index == 0:
                    # init cmd ---> searchcmd
                    input_stream = EsStream("es_stream", "localhost", 9200, "mytest")
                    cmd_opt.is_extract(True)

                if input_stream is None:
                    input_stream = LocalMemoryQueue('localQueue', 100)

                cmd_opt.set_input_stream(input_stream)

                output_stream = cmd_opt.out_stream
                if output_stream is None:
                    output_stream = LocalMemoryQueue('localQueue', 100)
                    cmd_opt.set_output_stream(output_stream)

                input_stream = output_stream
                end_stream = input_stream

                cmd_opt.set_lock(ex_lock)
                cmd_opt.set_exception(ex)

            return end_stream

        result_stream = stream_builder(opts, lock, exception)

        # schedule module
        from spl_arch.scheduler.demoScheduler import DemoScheduler
        scheduler = DemoScheduler("scheduler parallel", lock, exception)
        scheduler.schedule(opts)
        print(result_stream.pull())


if __name__ == "__main__":
    executor = Executor("spl", 0, 0)
    # executor.execute()
    executor.parallel_execute()
