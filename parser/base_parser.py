# -*- coding: utf-8 -*-
"""
    File Name: base_parser
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 17:51
"""


class BaseParser(object):
    def __init__(self, name, cmd):
        self.name = name
        self.cmd = cmd

    def validate(self):
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "validate"))

    def parse(self):
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "parse"))
