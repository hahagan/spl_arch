# -*- coding: utf-8 -*-
"""
    File Name: search_command
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 17:42
"""
from abc import ABC

from spl_arch.command.base_command import BaseCommand
from spl_arch.extract.comma_separator_extract import CommaSeparatorExtract
import logging


class SearchCommand(BaseCommand, ABC):
    def __init__(self, cmd_name, cmd_type, repo):
        super(SearchCommand, self).__init__(cmd_name, cmd_type)
        self.repo = repo
        self.in_stream = None
        self.out_stream = None
        self.extract = None

    def set_input_stream(self, in_stream):
        self.in_stream = in_stream

    def set_output_stream(self, out_stream):
        self.out_stream = out_stream

    def stream_in(self):
        return self.in_stream.pull()

    def is_extract(self, val):
        self.extract = val

    def calc(self):
        docs = self.stream_in()

        if self.extract:
            for doc in docs:
                raw = doc["_source"]["_raw"]
                extract_log = CommaSeparatorExtract("comma").extract_fields(raw, ["ch", "math", "en", "class"])
                doc["_source"]["_raw"] = extract_log

        self.set_output_stream(docs)

    def calculate(self):
        from spl_arch.stream.stream_exception import StreamFinishException
        try:
            while True:
                docs = self.stream_in()

                if self.extract:
                    for doc in docs:
                        raw = doc["_source"]["_raw"]
                        extract_log = CommaSeparatorExtract("comma").extract_fields(raw, ["ch", "math", "en", "class"])
                        doc["_source"]["_raw"] = extract_log

                self.out_stream.push(docs)
                self.out_stream.finish_in = True
        except StreamFinishException:
            pass
        except Exception as e:
            logging.exception(e)
            self._command_exception.set(e.args)
        finally:
            if self._exception_lock.locked(): self._exception_lock.release()
