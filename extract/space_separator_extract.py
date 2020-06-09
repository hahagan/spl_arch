# -*- coding: utf-8 -*-
"""
    File Name: space_separator_extract
    Description: ""
    Author: Donny.fang
    Date: 2020/6/9 11:09
"""
from spl_arch.extract.base_extract import BaseExtract


class SpaceSeparatorExtract(BaseExtract):
    """
    以空格作为分隔符，提取字段值
    """

    def __init__(self, name):
        super(SpaceSeparatorExtract, self).__init__(name)

    def extract_fields(self, log_line, fields):
        """

        :param log_line: "87 70 50 one"
        :param fields: ["ch", "en", "math", "class"]
        :return: {"ch":87, "en":70, "math":50, "class": "one"}
        """
        return dict(zip(fields, [e.strip() for e in log_line.split(" ") if e]))
