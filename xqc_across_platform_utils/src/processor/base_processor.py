#!/usr/bin/python3
import pickle
import threading

from abc import ABCMeta, abstractmethod
from typing import List, Tuple, Dict
from pulsar import Message

from log_utils.log_types import *
from log_utils.loggers import MsgProcessorLogger


class BaseMsgProcessor(metaclass=ABCMeta):
    def __init__(
            self, insert_batch_rows: int, logger: MsgProcessorLogger, name: str, *args, **kwargs
    ):
        self._rows_cache_dict: {str: List[Tuple]} = {}
        self._cache_rows_count = 0
        self.insert_batch_rows = insert_batch_rows
        self.logger = logger
        self.name = name
        self.table_batch_id: Dict[str, str] = dict()
        self.update_lock = threading.Lock()

    @abstractmethod
    def process_msg(self, msg: Message):
        """ Process every Pulsar message. """
        pass

    @abstractmethod
    def flush_cache_to_db(self):
        """ Flush Cached data to corresponding database. """
        pass
