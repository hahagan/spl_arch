# -*- coding: utf-8 -*-
"""
    File Name: mem_stream
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 16:51
"""
from spl_arch.stream.base_stream import BaseStream


class MemStream(BaseStream):
    def __init__(self, name):
        super(MemStream, self).__init__(name)

    def pull(self):
        pass

    def push(self):
        pass
