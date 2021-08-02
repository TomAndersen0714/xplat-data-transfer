#!/usr/bin/python3
import logging

from time import sleep
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

    def insert_data_with_tuple_list(self, table_name, rows_list) -> int:
        """
        Insert data into database(e.g. [{'a': 3, 'b': 'Tom'}], [(3,'Tom')]).
        """
        sql = f"insert into {table_name} values "
        insert_count = self.ch_client.execute(sql, rows_list)
        # print('insert into table %s: %d rows' % (table_name, insert_count))
        self.logger.info(
            'Insert into table %s: %d rows' % (table_name, insert_count), log_type=NORMAL_LOG
        )
        return insert_count

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
                # if insertion failed, dump the dirty data to dirty log
                self.logger.error('Insertion failed!' + '\n' + str(e), log_type=NORMAL_LOG)
                self.logger.error(tbl + ': ' + str(rows), log_type=DIRTY_LOG)

    def clear_table(self, properties=None):
        """
        Clear specified table or partition.
        """
        if properties is None:
            return

        clear_table = properties.get('clear_table', None)
        batch_id = properties.get('batch_id', None)

        if clear_table and batch_id:
            # ensure clear exactly-once and execute in the first arrived thread using double check lock
            if clear_table not in self.table_batch_id \
                    or self.table_batch_id[clear_table] != batch_id:

                with self.update_lock:
                    if clear_table not in self.table_batch_id \
                            or self.table_batch_id[clear_table] != batch_id:

                        self.table_batch_id[clear_table] = batch_id
                        # update specified partition or table
                        partition = properties.get('partition', None)
                        cluster_name = properties.get('cluster_name', None)
                        if partition:
                            ch_del_sql = "ALTER TABLE %s {cluster} DROP PARTITION %s" % (clear_table, partition)
                        else:
                            ch_del_sql = "TRUNCATE TABLE %s {cluster}" % clear_table

                        if cluster_name:
                            ch_del_sql = ch_del_sql.format(cluster=f"ON CLUSTER {cluster_name}")
                        else:
                            ch_del_sql = ch_del_sql.format(cluster="")

                        self.ch_client.execute(ch_del_sql)
                        self.logger.info(ch_del_sql, log_type=NORMAL_LOG)
                        sleep(3)
