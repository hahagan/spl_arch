# -*- coding: utf-8 -*-
"""
    File Name: abstract_stream
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 16:04
"""


class BaseStream(object):
    def __init__(self, name):
        self.name = name

    def pull(self):
        """
        get data
        :return:
        """
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "pull"))

    def push(self):
        """
        push data
        :return:
        """
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "push"))

    def clean(self):
        """
        clean stream's resources
        :return:
        """
        pass
