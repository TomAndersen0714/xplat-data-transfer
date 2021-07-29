#!/usr/bin/python3
import logging
from logging.handlers import TimedRotatingFileHandler

from log_utils.filters import TypeFilter
from log_utils.loggers import MsgProcessorLogger
from log_utils.log_types import *
from log_utils.handlers import TimedAndSizeRotatingHandler


def test_handler():
    level = logging.INFO
    fmt_without_timestamp = logging.Formatter("%(message)s")
    logger = MsgProcessorLogger(name='test', level=level)

    wal_log_handler = TimedAndSizeRotatingHandler(
        filename='test.log', when='D', interval=1, backupCount=7
    )
    wal_log_handler.setLevel(level)
    wal_log_handler.setFormatter(fmt_without_timestamp)
    wal_log_handler.addFilter(TypeFilter(WAL_LOG, 'wal'))
    logger.addHandler(wal_log_handler)

    logger.info('test wal log', log_type=WAL_LOG)


def test_logger():
    level = logging.INFO
    fmt_without_timestamp = logging.Formatter("%(message)s")
    logger = MsgProcessorLogger(name='test', level=level)
    dirty_data_log_handler = TimedRotatingFileHandler(
        filename='test_dirty.log', when='D', interval=1, backupCount=7
    )
    dirty_data_log_handler.setLevel(level)
    dirty_data_log_handler.setFormatter(fmt_without_timestamp)
    dirty_data_log_handler.addFilter(TypeFilter(DIRTY_LOG, 'dirty'))
    logger.addHandler(dirty_data_log_handler)

    logger.error('test', log_type=DIRTY_LOG)


def test_logger_1():
    level = logging.INFO
    fmt_with_timestamp = logging.Formatter(
        '[%(asctime)s] '
        '[%(pathname)s] \n'
        '{%(filename)s:%(lineno)d, process:%(processName)s-%(process)d, thread:%(threadName)s-%(thread)d} '
        '%(levelname)s - %(message)s'
    )
    logger = MsgProcessorLogger(name='test', level=level)
    dirty_data_log_handler = TimedRotatingFileHandler(
        filename='test_dirty.log', when='D', interval=1, backupCount=7
    )
    dirty_data_log_handler.setLevel(level)
    dirty_data_log_handler.setFormatter(fmt_with_timestamp)
    dirty_data_log_handler.addFilter(TypeFilter(DIRTY_LOG, 'dirty'))
    logger.addHandler(dirty_data_log_handler)

    logger.error('test', log_type=DIRTY_LOG)


def test_root_logger():
    FORMAT = '%(module)s %(funcName)s %(pathname)s %(funcName)s: %(message)s'
    logging.basicConfig(
        format=FORMAT, level=logging.INFO
    )
    logging.info('This is a test log info.')
    print(logging.root.findCaller(stack_info=False))


if __name__ == '__main__':
    test_logger_1()
