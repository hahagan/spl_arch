# -*- coding: utf-8 -*-
"""
    File Name: StatsCommand
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 22:21
"""
import logging
import random
from abc import ABC
from spl_arch.command.base_command import BaseCommand
from spl_arch.function.avg import Avg


class StatsCommand(BaseCommand, ABC):
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

    # def calc(self):
    #     from spl_arch.utils.utils import Field_Names
    #     docs, d = self.in_stream, {}
    #     self.group_field = Field_Names[-1] if len(Field_Names) > 0 else ""
    #     self.field = Field_Names[random.randint(0, len(Field_Names) - 1)] if len(Field_Names) > 0 else ""
    #     self.as_field = "as_{}".format(self.field)
    #
    #     for doc in docs:
    #         raw = doc["_source"]["_raw"]
    #
    #         if raw[self.group_field] not in d:
    #             d[raw[self.group_field]] = []
    #
    #         d[raw[self.group_field]].append(int(raw[self.field]))
    #
    #     for _group, _sum in d.items():
    #         self.output.append({self.group_field: _group, self.as_field: Avg("avg", "agg").avg(sum(_sum), len(_sum))})
    #
    #     self.set_output_stream(self.output)

    def calculate(self):
        from spl_arch.stream.stream_exception import StreamFinishException
        from spl_arch.utils.utils import Field_Names
        d = dict()
        try:
            while True:
                docs = self.in_stream.pull()
                self.group_field = Field_Names[-1] if len(Field_Names) > 0 else ""
                self.field = Field_Names[random.randint(0, len(Field_Names) - 2)] if len(Field_Names) > 0 else ""
                self.as_field = "as_{}".format(self.field)

                for doc in docs:
                    raw = doc["_source"]["_raw"]

                    if raw[self.group_field] not in d:
                        d[raw[self.group_field]] = []

                    d[raw[self.group_field]].append(int(raw[self.field]))

        except StreamFinishException:
            # get total data
            for _group, _sum in d.items():
                self.output.append(
                    {self.group_field: _group, self.as_field: Avg("avg", "agg").avg(sum(_sum), len(_sum))})

            self.out_stream.push(self.output)
            self.out_stream.finish_in = True
        except Exception as e:
            logging.exception(e)
            self._command_exception.set(e.args)
        finally:
            if self._exception_lock.locked(): self._exception_lock.release()