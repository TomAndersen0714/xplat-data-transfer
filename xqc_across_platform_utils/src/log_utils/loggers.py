#!/usr/bin/python3
# @Author   : chengcheng@xiaoduotech.com
# @Date     : 2021/07/21

import logging
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
