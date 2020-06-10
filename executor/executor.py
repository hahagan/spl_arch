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
from spl_arch.stream.mysql.mysql_stream import MysqlStream
from spl_arch.scheduler.demo_scheduler import DemoScheduler
from spl_arch.scheduler.command_exception import CommandException
import json
from threading import Lock


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

        # encapsulate cmd_opt object and build input_output stream
        def stream_builder(cmd_opts, ex_lock, ex, stream_class):
            input_stream = None
            end_stream = None
            count = 0

            for _index, cmd_opt in enumerate(cmd_opts):
                if _index == 0:
                    # init cmd ---> searchcmd
                    input_stream = EsStream("es_stream", "localhost", 9200, "mytest")
                    cmd_opt.is_extract(True)

                if input_stream is None:
                    count += 1
                    input_stream = stream_class("stream" + str(count), 100)

                cmd_opt.set_input_stream(input_stream)

                output_stream = cmd_opt.out_stream
                if output_stream is None:
                    count += 1
                    output_stream = input_stream = stream_class("stream" + str(count), 100)
                    cmd_opt.set_output_stream(output_stream)

                input_stream = output_stream
                end_stream = input_stream

                cmd_opt.set_lock(ex_lock)
                cmd_opt.set_exception(ex)

            return end_stream

        lock = Lock()
        exception = CommandException(lock)
        result_stream = stream_builder(opts, lock, exception, LocalMemoryQueue)
        # result_stream = stream_builder(opts, lock, exception, MysqlStream)

        # schedule module
        scheduler = DemoScheduler("scheduler parallel", lock, exception)
        scheduler.schedule(opts)

        if scheduler.status == scheduler.FINISH:
            print(json.dumps(result_stream.pull(), indent=4, separators=(",", ":")))
        else:
            print("Executor error!!!")


if __name__ == "__main__":
    executor = Executor("spl", 0, 0)
    # executor.execute()
    executor.parallel_execute()