# -*- coding: utf-8 -*-
"""
    File Name: avg
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 23:09
"""
from spl_arch.function.base_func import Function


class Avg(Function):
    def __init__(self, func_name, func_type):
        super(Avg, self).__init__(func_name)
        self.func_type = func_type

    def avg(self, total, counter):
        return total / counter
