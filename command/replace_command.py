# -*- coding: utf-8 -*-
"""
    File Name: replace_command
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 15:08
"""
import logging
from abc import ABC
from spl_arch.command.base_command import BaseCommand


class ReplaceCommand(BaseCommand, ABC):
    def __init__(self, cmd_name, cmd_type, val, replace_val, field):
        super(ReplaceCommand, self).__init__(cmd_name, cmd_type)
        self.val = val
        self.replace_val = replace_val
        self.field = field
        self.in_stream, self.out_stream = None, None

    def set_input_stream(self, in_stream):
        self.in_stream = in_stream

    def set_output_stream(self, out_stream):
        self.out_stream = out_stream

    def stream_in(self):
        return self.in_stream

    # def calc(self):
    #     from spl_arch.utils.utils import Field_Names
    #     docs = self.in_stream
    #     self.field = Field_Names[-1] if len(Field_Names) > 0 else ""
    #
    #     for doc in docs:
    #         raw = doc["_source"]["_raw"]
    #
    #         if raw[self.field] == self.val:
    #             raw[self.field] = self.replace_val
    #
    #     self.set_output_stream(docs)

    def calculate(self):
        from spl_arch.stream.stream_exception import StreamFinishException
        from spl_arch.utils.utils import Field_Names
        self.field = Field_Names[-1] if len(Field_Names) > 0 else ""
        try:
            while True:
                docs = self.in_stream.pull()

                for doc in docs:
                    raw = doc["_source"]["_raw"]

                    if raw[self.field] == self.val:
                        raw[self.field] = self.replace_val

                self.out_stream.push(docs)
        except StreamFinishException:
            self.out_stream.finish_in = True
        except Exception as e:
            logging.exception(e)
            self._command_exception.set(e.args)
        finally:
            if self._exception_lock.locked(): self._exception_lock.release()
