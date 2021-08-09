#!/usr/bin/python3
# @Author   : chengcheng@xiaoduotech.com
# @Date     : 2021/07/20

import logging
from .log_types import *


class SuffixFilter(logging.Filter):
    """
    Log filter for filtering log record with specific suffix.
    """

    def __init__(self, name, suffix: str):
        logging.Filter.__init__(self, name)
        self.suffix = suffix

    def filter(self, record: logging.LogRecord):
        """
        If filter get the message with specific suffix, filter will remove the suffix and pass the
        record, else return false(i.e. ignore this record).
        """
        idx = record.msg.find(self.suffix)
        if idx == 0:
            record.msg = record.msg[len(self.suffix):]  # remove the suffix
            return True
        else:
            return False


class ExtraInfoFilter(logging.Filter):
    """
    Log filter for filtering log record with specific extra info.
    """

    def __init__(self, expected_attr_key: str, expected_attr_value, name):
        logging.Filter.__init__(self, name)
        self.expected_attr_key = expected_attr_key
        self.expected_attr_value = expected_attr_value

    def filter(self, record: logging.LogRecord):
        """
        If filter get the message with specific extra info, return true.
        """
        if self.expected_attr_key in record.__dict__ and \
                record.__dict__[self.expected_attr_key] == self.expected_attr_value:
            return True
        else:
            return False


class AttrFilter(logging.Filter):
    """
    Log filter for filtering log record with specific type in record.__dict__.
    """

    def __init__(self, expected_attr_key: str, expected_attr_value, name):
        logging.Filter.__init__(self, name)
        self.expected_attr_key = expected_attr_key
        self.expected_attr_value = expected_attr_value

    def filter(self, record: logging.LogRecord):
        """
        If filter get the message with specific extra info, return true.
        """
        if self.expected_attr_key in record.__dict__ and \
                record.__dict__[self.expected_attr_key] == self.expected_attr_value:
            return True
        else:
            return False


class TypeFilter(logging.Filter):
    """
    Log filter for filtering log record with specific K-V in args.
    """

    def __init__(self, log_type: int, name):
        logging.Filter.__init__(self, name)
        self.log_type = log_type

    def filter(self, record: logging.LogRecord):
        """
        If filter get the message with specific k-v in args, return true.
        """
        if not isinstance(record.args, dict):
            if isinstance(record.args[0], dict):
                kvs = record.args[0]
                if TYPE in kvs and kvs[TYPE] == self.log_type:
                    return True
                else:
                    return False
            return False

        if TYPE in record.args and record.args[TYPE] == self.log_type:
            return True
        else:
            return False
