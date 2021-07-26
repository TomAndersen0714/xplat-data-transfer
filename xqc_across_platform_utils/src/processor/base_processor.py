#!/usr/bin/python3
import pickle

from abc import ABCMeta, abstractmethod
from typing import List, Tuple
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

    def process_msg(self, msg: Message):
        # parse the message
        content = msg.data()
        properties = msg.properties()
        msg_id = msg.message_id()
        target_table = properties.get('target_table', None)

        # deserialize the data from message
        if target_table:
            try:
                self.logger.info(f'Processing message:{msg_id}', log_type=NORMAL_LOG)
                rows_bytes_list = pickle.loads(content)
                msg_rows_list = [pickle.loads(rows_bytes_list[i])
                                 for i in range(len(rows_bytes_list))]
            except Exception as e:
                # if pickle module cannot deserialize the message, dump it to bad_message log
                # and process next message
                self.logger.error(str(e), log_type=NORMAL_LOG)
                self.logger.error(str(msg_id) + ' - ' + str(properties) +
                                  ':' + str(content), log_type=BAD_MSG_LOG)
                return

            # if pickle module deserialize message successfully, dump it to wal log
            self.logger.info(str(properties) + ':' + str(msg_rows_list), log_type=WAL_LOG)

            # allocate space and cache the records
            if target_table not in self._rows_cache_dict:
                self._rows_cache_dict[target_table] = []

            if self._rows_cache_dict[target_table]:
                self._rows_cache_dict[target_table] += msg_rows_list
            else:
                self._rows_cache_dict[target_table] = msg_rows_list
            self._cache_rows_count += len(msg_rows_list)

            # check the cache threshold
            if self._cache_rows_count >= self.insert_batch_rows:
                self.flush_cache_to_db()

        else:
            # raise TypeError('Target table is not specified!')
            # the properties of message doesn't contain 'target_table'
            self.logger.error('Target table is not specified in properties!' +
                              str(msg_id) + ' - ' + str(properties), log_type=NORMAL_LOG)
            # dump this message to bad message log
            self.logger.error(str(msg_id) + ' - ' + str(properties) +
                              ':' + str(content), log_type=BAD_MSG_LOG)

    @abstractmethod
    def flush_cache_to_db(self):
        pass
