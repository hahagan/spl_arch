# -*- coding: utf-8 -*-
"""
    File Name: utils
    Description: ""
    Author: Donny.fang
    Date: 2020/6/4 15:14
"""
from spl_arch.extract.comma_separator_extract import CommaSeparatorExtract
from spl_arch.extract.space_separator_extract import SpaceSeparatorExtract

Field_Names = []


# def overrides(interface_class):
#     """
#     Function override annotation.
#     Corollary to @abc.abstractmethod where the override is not of an
#     abstractmethod.
#     Modified from answer https://stackoverflow.com/a/8313042/471376
#     """
#
#     def confirm_override(method):
#         if method.__name__ not in dir(interface_class):
#             raise NotImplementedError('function "%s" is an @override but that'
#                                       ' function is not implemented in base'
#                                       ' class %s'
#                                       % (method.__name__,
#                                          interface_class)
#                                       )
#
#         def func():
#             pass
#
#         attr = getattr(interface_class, method.__name__)
#         if type(attr) is not type(func):
#             raise NotImplementedError('function "%s" is an @override'
#                                       ' but that is implemented as type %s'
#                                       ' in base class %s, expected implemented'
#                                       ' type %s'
#                                       % (method.__name__,
#                                          type(attr),
#                                          interface_class,
#                                          type(func))
#                                       )
#         return method
#
#     return confirm_override

def extract(log_sample: str):
    field_names_list, extract_obj = [], None
    if log_sample is not None:
        # have data
        delimiter_dict, counter = {"comma": ","}, 5

        while counter > 0:
            print("Sample log: {}".format(log_sample))

            while 1:
                delimiter = input("Currently supports fixed delimiters[comma, ...], Please choose: ").strip()

                if delimiter in delimiter_dict:
                    break

                print("Input error, please choose in [comma, ...]")

            field_names = input("Please input field name, use comma as sepator: ").strip()
            field_names_list = [v.strip() for v in field_names.split(delimiter_dict[delimiter]) if v]
            sample_log_field_list = [e.strip() for e in log_sample.split(delimiter_dict[delimiter]) if e]

            if len(field_names_list) != len(sample_log_field_list):
                print("The number of field_value and field_name must match, please check...")
                counter -= 1
                continue
            else:
                # match success
                extract_obj = CommaSeparatorExtract("comma") if delimiter.strip() == "comma" \
                    else SpaceSeparatorExtract("space")
                break

    global Field_Names
    Field_Names = field_names_list

    return {
        "obj": extract_obj,
        "names": field_names_list
    }
