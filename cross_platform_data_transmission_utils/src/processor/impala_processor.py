import logging
import pickle

from time import sleep
from pulsar import Message
from impala.dbapi import connect
from impala.hiveserver2 import HiveServer2Connection

from log_utils.log_types import *
from .base_processor import BaseMsgProcessor


class ImpalaProcessor(BaseMsgProcessor):

    def __init__(
            self, hs2_host='localhost', hs2_port=21050, insert_batch_rows=50000, logger=logging.root,
            *args, **kwargs
    ):
        BaseMsgProcessor.__init__(
            self, insert_batch_rows=insert_batch_rows, logger=logger, name='impala',
            *args, **kwargs
        )
        self.hs2_connect: HiveServer2Connection = connect(hs2_host, hs2_port)
        self.cursor = self.hs2_connect.cursor()

    def flush_cache_to_db(self):
        """
        Flush cached data into database.
        """
        # self.logger.info('Trying to flush cached data to ClickHouse!', log_type=NORMAL_LOG)
        table_names = list(self._rows_cache_dict.keys())
        if len(table_names) == 0:
            # self.logger.info('The cache is empty, and there is no data to flush.', log_type=NORMAL_LOG)
            return
        for tbl in table_names:
            # what if some insertion failed?
            rows = self._rows_cache_dict.pop(tbl)
            try:
                res = self.insert_data_with_tuple_list(tbl, rows)
                self._cache_rows_count -= res
            except Exception as e:
                self._cache_rows_count -= len(rows)
                # if insertion failed, dump the dirty data to dirty log
                self.logger.error(f"Insertion failed!\n{e}", log_type=NORMAL_LOG)
                self.logger.error(f"{tbl}: {rows}", log_type=DIRTY_LOG)

    def clear_table(self, properties=None):
        pass

    def par_exp_formatter(self, partition):
        pass

    def str_fmt(s):
        """ Add single quotation marks for string. """
        return "'%s'" % s
