# -*- coding: utf-8 -*-
"""
    File Name: StatsCommand
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 22:21
"""
from spl_arch.command.base_command import BaseCommand
from spl_arch.function.avg import Avg


class StatsCommand(BaseCommand):
    def __init__(self, cmd_name, cmd_type, func, field, as_field, group_field):
        super(StatsCommand, self).__init__(cmd_name, cmd_type)
        self.func = func
        self.field = field
        self.as_field = as_field
        self.group_field = group_field
        self.in_stream = None
        self.out_stream = None
        self.output = []

    def set_input_stream(self, in_stream):
        self.in_stream = in_stream

    def set_output_stream(self, out_stream):
        self.out_stream = out_stream

    def stream_in(self):
        return self.in_stream

    def calc(self):
        docs, d = self.in_stream, {}

        for doc in docs:
            raw = doc["_source"]["_raw"]

            if raw["class"] not in d:
                d[raw["class"]] = []

            d[raw["class"]].append(int(raw["math"]))

        for _class, _sum in d.items():
            self.output.append({"class": _class, self.as_field: Avg("avg", "agg").avg(sum(_sum), len(_sum))})

        self.set_output_stream(self.output)
