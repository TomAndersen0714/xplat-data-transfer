#!/usr/bin/python3
import logging

from clickhouse_driver import Client

from log_utils.log_types import *
from .base_processor import BaseMsgProcessor


class ClickHouseProcessor(BaseMsgProcessor):

    def __init__(
            self, ch_host, ch_port, insert_batch_rows=10000, logger=logging.root,
            *args, **kwargs
    ):
        super(ClickHouseProcessor, self).__init__(
            insert_batch_rows=insert_batch_rows, logger=logger, name='clickhouse', *args, **kwargs)
        self.cluster_name = kwargs.get('ch_cluster_name', 'cluster_3s_2r')
        self.ch_client = Client(ch_host, ch_port)

    def insert_data_with_tuple_list(self, table_name, dict_list) -> int:
        sql = f"insert into {table_name} values "
        insert_count = self.ch_client.execute(sql, dict_list)
        # print('insert into table %s: %d rows' % (table_name, insert_count))
        self.logger.info(
            'Insert into table %s: %d rows' % (table_name, insert_count), log_type=NORMAL_LOG
        )
        return insert_count

    def flush_cache_to_db(self):
        """
        Flush cached data into database.
        """
        self.logger.info('Trying to flush cached data to ClickHouse!', log_type=NORMAL_LOG)
        table_names = list(self._rows_cache_dict.keys())
        if len(table_names) == 0:
            self.logger.info('Cache is empty, there is no data to flush.', log_type=NORMAL_LOG)
        for tbl in table_names:
            # what if some insertion failed?
            rows = self._rows_cache_dict.pop(tbl)
            try:
                res = self.insert_data_with_tuple_list(tbl, rows)
                self._cache_rows_count -= res
            except Exception as e:
                # if insertion failed, dump the dirty data to dirty log
                self.logger.error('Insertion failed!' + '\n' + str(e), log_type=NORMAL_LOG)
                self.logger.error(tbl + ':' + rows, log_type=DIRTY_LOG)
