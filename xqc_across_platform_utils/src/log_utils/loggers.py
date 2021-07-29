#!/usr/bin/python3
# @Author   : chengcheng@xiaoduotech.com
# @Date     : 2021/07/21
import io
import logging
import os
import sys
import inspect
import traceback

from .log_types import *


class MsgProcessorLogger(logging.Logger):
    """
    A custom logger for pulsar consumer.
    """

    def __init__(self, name, level):
        logging.Logger.__init__(self, name, level)

    def info(self, msg, log_type=NORMAL_LOG, *args, **kwargs):
        super().info(msg, {TYPE: log_type}, *args, **kwargs)

    def warning(self, msg, log_type=NORMAL_LOG, *args, **kwargs):
        super().warning(msg, {TYPE: log_type}, *args, **kwargs)

    def error(self, msg, log_type=NORMAL_LOG, *args, **kwargs):
        super().error(msg, {TYPE: log_type}, *args, **kwargs)

    def critical(self, msg, log_type=NORMAL_LOG, *args, **kwargs):
        super().critical(self, {TYPE: log_type}, msg, *args, **kwargs)

    def findCaller(self, stack_info=False):
        """
        Find the stack frame of the caller so that we can note the source
        file name, line number and function name.
        """
        f = inspect.currentframe()
        rv = "(unknown file)", 0, "(unknown function)", None

        if f is not None:
            # get the stack frame of the caller of info/warning/error/critical method
            f = f.f_back.f_back.f_back.f_back
        if hasattr(f, "f_code"):
            # get the absolute filename, line number, and function name of caller
            co = f.f_code
            rv = (co.co_filename, f.f_lineno, co.co_name, None)
        return rv
