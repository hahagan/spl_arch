# -*- coding: utf-8 -*-
"""
    File Name: scheduler
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 19:23
"""
import json
from spl_arch.stream.es_stream import EsStream


class Scheduler(object):
    def __init__(self, name):
        self.name = name

    def schedule(self, cmd_opts):
        """
        schedule ...
        :param cmd_opts:
        :return:
        """
        input_stream, end = None, None

        for _index, cmd_opt in enumerate(cmd_opts):
            if _index == 0:
                # init cmd ---> searchcmd
                input_stream = EsStream("es_stream", "localhost", 9200, "mytest")
                cmd_opt.is_extract(True)

            cmd_opt.set_input_stream(input_stream)
            cmd_opt.calc()

            input_stream = cmd_opt.out_stream
            end = cmd_opt

        print(json.dumps(end.out_stream, indent=4, separators=(",", ":")))

    def paralell_schedule(self):
        pass
