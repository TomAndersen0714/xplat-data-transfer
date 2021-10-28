#!/usr/bin/python3
import logging, os, sys

from logging.handlers import TimedRotatingFileHandler
from typing import Dict
from .filters import ExtraInfoFilter, TypeFilter
from .loggers import MsgProcessorLogger
from .handlers import TimedAndSizeRotatingHandler
from .log_types import *

__all__ = ['init_root_logger', 'get_msg_processor_logger']
__logger_dict__: Dict[str, logging.Logger] = dict()

fmt_with_timestamp = logging.Formatter(
    '[%(asctime)s] '
    '{%(filename)s:%(lineno)d, process:%(processName)s-%(process)d, thread:%(threadName)s-%(thread)d} '
    '%(levelname)s - %(message)s'
)
fmt_without_timestamp = logging.Formatter(
    "%(message)s"
)


def check_and_make_dir(base_path):
    if os.path.exists(base_path):
        logging.info(f"'{base_path}' already exists!")
        if not os.path.isdir(base_path):
            error = f"'{base_path}' is not directory!"
            logging.error(error)
            raise FileExistsError(error)
    else:
        try:
            os.makedirs(base_path)
        except Exception as e:
            logging.error(str(e))
            sys.exit(-1)


def check_file(filepath):
    if os.path.exists(filepath):
        logging.info(f"'{filepath}' already exists!")

    if os.path.isdir(filepath):
        error = f"'{filepath}' is directory but not normal file!"
        logging.error(error)
        raise FileExistsError(error)


def init_root_logger(
        base_path, filename='sys.log', level=logging.DEBUG,
        fmt: logging.Formatter = fmt_with_timestamp, when='D', encoding='utf-8', backupCount=7
):
    """
    Set basic config for root logger to store sys runtime log.
    """

    # check the file and directory path
    if os.path.exists(base_path):
        print(f"'{base_path}' already exists!")
        if not os.path.isdir(base_path):
            raise FileExistsError(f"'{base_path}' is not directory!")
    else:
        try:
            os.makedirs(base_path)
        except Exception as e:
            print(str(e))
            sys.exit(-1)

    # set the basic config for root logger
    base_file = os.path.join(base_path, filename)

    handler = TimedRotatingFileHandler(
        filename=base_file, when=when, backupCount=backupCount, encoding=encoding
    )
    handler.setLevel(level)
    handler.setFormatter(fmt)
    logging.root.addHandler(handler)
    logging.root.setLevel(level)

    # # check the base path the filename after configuration
    # # to avoid creating default stderr stream handler
    # check_and_make_dir(base_path)
    # check_file(base_file)


def get_msg_processor_logger(
        logger_name, base_path, level: int = logging.DEBUG,
        when='MIDNIGHT', interval=1, encoding='utf-8', backupCount=7
):
    """
    Create a logger for pulsar message processor, and default shelf life of log files is 7 days.
    :param backupCount:
    :param encoding:
    :param interval:
    :param when:
    :param logger_name:
    :param base_path:
    :param level:
    """
    # check the cache
    if logger_name in __logger_dict__:
        return __logger_dict__[logger_name]

    # check the file and directory path
    check_and_make_dir(base_path)

    # create pulsar consumer logger and add handlers and filters into it
    logger = MsgProcessorLogger(name=logger_name, level=level)

    # add normal log handler and filter
    normal_log_dir = os.path.join(base_path, 'normal')
    check_and_make_dir(normal_log_dir)
    normal_log_base_file = os.path.join(normal_log_dir, 'normal.log')
    check_file(normal_log_base_file)
    normal_log_handler = TimedRotatingFileHandler(
        filename=normal_log_base_file, when=when, interval=interval,
        encoding=encoding, backupCount=backupCount
    )
    normal_log_handler.setLevel(level)
    normal_log_handler.setFormatter(fmt_with_timestamp)
    normal_log_handler.addFilter(TypeFilter(NORMAL_LOG, 'normal'))
    logger.addHandler(normal_log_handler)

    # add wal log handler and filter
    wal_log_dir = os.path.join(base_path, 'wal')
    check_and_make_dir(wal_log_dir)
    wal_log_base_file = os.path.join(wal_log_dir, 'wal.log')
    check_file(wal_log_base_file)
    wal_log_handler = TimedAndSizeRotatingHandler(
        filename=wal_log_base_file, when=when, interval=interval,
        encoding=encoding, backupCount=backupCount
    )
    wal_log_handler.setLevel(level)
    wal_log_handler.setFormatter(fmt_without_timestamp)
    wal_log_handler.addFilter(TypeFilter(WAL_LOG, 'wal'))
    logger.addHandler(wal_log_handler)

    # add bad message log handler and filter
    bad_msg_log_dir = os.path.join(base_path, 'bad_msg')
    check_and_make_dir(bad_msg_log_dir)
    bad_msg_log_base_file = os.path.join(bad_msg_log_dir, 'bad_msg.log')
    check_file(bad_msg_log_base_file)
    bad_msg_log_handler = TimedRotatingFileHandler(
        filename=bad_msg_log_base_file, when=when, interval=interval,
        encoding=encoding, backupCount=backupCount
    )
    bad_msg_log_handler.setLevel(level)
    bad_msg_log_handler.setFormatter(fmt_with_timestamp)
    bad_msg_log_handler.addFilter(TypeFilter(BAD_MSG_LOG, 'bad_msg'))
    logger.addHandler(bad_msg_log_handler)

    # add dirty log handler and filters
    dirty_data_log_dir = os.path.join(base_path, 'dirty')
    check_and_make_dir(dirty_data_log_dir)
    dirty_data_log_base_file = os.path.join(dirty_data_log_dir, 'dirty.log')
    check_file(dirty_data_log_base_file)
    dirty_data_log_handler = TimedRotatingFileHandler(
        filename=dirty_data_log_base_file, when=when, interval=interval,
        encoding=encoding, backupCount=backupCount
    )
    dirty_data_log_handler.setLevel(level)
    dirty_data_log_handler.setFormatter(fmt_without_timestamp)
    dirty_data_log_handler.addFilter(TypeFilter(DIRTY_LOG, 'dirty'))
    logger.addHandler(dirty_data_log_handler)

    __logger_dict__[logger_name] = logger
    return logger
