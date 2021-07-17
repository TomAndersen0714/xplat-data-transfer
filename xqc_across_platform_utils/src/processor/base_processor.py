#!/usr/bin/python3
import pickle
from typing import List, Tuple
from pulsar import Message
from abc import ABCMeta, abstractmethod


class BaseMsgProcessor(metaclass=ABCMeta):
    def __init__(self, insert_batch_rows, *args, **kwargs):
        self._rows_cache_dict: {str: List[Tuple]} = {}
        self._cache_rows_count = 0
        self.insert_batch_rows = insert_batch_rows

    def process_msg(self, msg: Message):
        # parse the message
        properties = msg.properties()
        target_table = properties.get('target_table', '')

        # deserialize the data from message
        if target_table:
            content = msg.data()
            rows_bytes_list = pickle.loads(content)
            msg_rows_list = [pickle.loads(rows_bytes_list[i])
                             for i in range(len(rows_bytes_list))]

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
            raise TypeError('Target table is not specified!')

    @abstractmethod
    def flush_cache_to_db(self):
        pass
