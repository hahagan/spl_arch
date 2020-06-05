# -*- coding: utf-8 -*-
"""
    File Name: search_command
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 17:42
"""
from spl_arch.command.base_command import BaseCommand
from spl_arch.extract.comma_separator_extract import CommaSeparatorExtract


class SearchCommand(BaseCommand):
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
