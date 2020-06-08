# -*- coding: utf-8 -*-
"""
    File Name: BaseCommand
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 14:55
"""


class BaseCommand(object):
    def __init__(self, cmd_name, cmd_type, exception_lock=None, exception=None):
        self.cmd_name = cmd_name
        self.cmd_type = cmd_type
        self._exception_lock = exception_lock
        self._command_exception = exception

    def set_lock(self, ex_lock):
        self._exception_lock = ex_lock

    def set_exception(self, exception):
        self._command_exception = exception

    def set_input_stream(self, in_stream):
        """
        set input stream for cmd
        :return:
        """
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "set_input_stream"))

    def set_output_stream(self, out_stream):
        """
        set output stream for cmd
        :param out_stream:
        :return:
        """
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "set_output_stream"))

    def stream_in(self):
        """
        stream in
        :return:
        """
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "stream_in"))

    def write_to(self):
        """
        write to
        :return:
        """
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "write_to"))

    def calc(self):
        """
        cmd calculate
        :return:
        """
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "calc"))

    def calculate(self):
        """
        cmd calculate
        :return:
        """
        raise NotImplementedError("{} {} not implemented".format(self.__class__.__name__, "calculate"))
