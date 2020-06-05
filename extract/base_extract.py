# -*- coding: utf-8 -*-
"""
    File Name: base_extract
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 17:04
"""


class BaseExtract(object):
    def __init__(self, name):
        self.name = name

    def extract_fields(self, log_line, fields):
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "extract_fields"))
