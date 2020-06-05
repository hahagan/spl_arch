# -*- coding: utf-8 -*-
"""
    File Name: parser
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 17:49
"""
from spl_arch.parser.base_parser import BaseParser
from spl_arch.command.search_command import SearchCommand
from spl_arch.command.replace_command import ReplaceCommand
from spl_arch.command.stats_command import StatsCommand


class AntlrParser(BaseParser):
    def __init__(self, name, cmd):
        super(AntlrParser, self).__init__(name, cmd)
        self.cmd_opts = []

    def validate(self):
        pass

    def parse(self):
        search_cmd = SearchCommand("search", "streaming", "mytest")
        replace_cmd = ReplaceCommand("replace", "streaming", "one", "oneClass", "class")
        stats_cmd = StatsCommand("stats", "non_streaming", "avg", "math", "avg_math", "class")

        self.cmd_opts.append(search_cmd)
        self.cmd_opts.append(replace_cmd)
        self.cmd_opts.append(stats_cmd)

        return self.cmd_opts
