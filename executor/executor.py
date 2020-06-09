# -*- coding: utf-8 -*-
"""
    File Name: executor
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 14:04
"""
import json
from threading import Lock
from spl_arch.input.spl_input import SplInput
from spl_arch.parser.antlr_parser import AntlrParser
from spl_arch.scheduler.scheduler import Scheduler
from spl_arch.stream.es_stream import EsStream
from spl_arch.stream.local_mem_stream import LocalMemoryQueue
from spl_arch.scheduler.command_exception import CommandException


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

        此处采用lock对象共享的方式来控制每个命令的并行处理
        main_thread首先会创建一个lock对象，每个命令对应的操作对象分别都设置持有lock，比如cmd_opt.set_lock(lock)
        main_thread中，比如三个命令并行处理，分别对应着三个线程；按照规约，search会首先执行，等到其执行完毕，会做lock的release操作；
        紧接着main_thread会aquire这个lock，然后往下执行，直到重新aquire，然后block住main_thread；此时意味着main_thread会等待着
        其他的命令执行结束，release lock，这样main_thread又会被重新唤起，接着往下执行；如此反复，直到最后一个命令执行完毕即可。

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

        # encapsulate cmd_opt object and build input_output stream
        def stream_builder(cmd_opts, ex_lock, ex):
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

        lock = Lock()
        exception = CommandException(lock)
        result_stream = stream_builder(opts, lock, exception)

        # schedule module
        from spl_arch.scheduler.demo_scheduler import DemoScheduler
        scheduler = DemoScheduler("scheduler parallel", lock, exception)
        scheduler.schedule(opts)

        print(json.dumps(result_stream.pull(), indent=4, separators=(",", ":")))


if __name__ == "__main__":
    executor = Executor("spl", 0, 0)
    # executor.execute()
    executor.parallel_execute()
