#!/usr/bin/python3
from clickhouse_driver import Client
from .base_processor import BaseMsgProcessor


class ClickHouseProcessor(BaseMsgProcessor):

    def __init__(
            self, ch_host, ch_port, insert_batch_rows=10000,
            *args, **kwargs
    ):
        super(ClickHouseProcessor, self).__init__(insert_batch_rows, *args, **kwargs)
        self.cluster_name = kwargs.get('ch_cluster_name', 'cluster_3s_2r')
        self.ch_client = Client(ch_host, ch_port)

    def insert_data_with_tuple_list(self, table_name, dict_list) -> int:
        sql = f"insert into {table_name} values "
        insert_count = self.ch_client.execute(sql, dict_list)
        print('insert into table %s: %d rows' % (table_name, insert_count))
        return insert_count

    def flush_cache_to_db(self):
        """
        Flush cached data into database.
        """
        table_names = list(self._rows_cache_dict.keys())
        for tbl in table_names:
            # what if some insertion failed?
            res = self.insert_data_with_tuple_list(tbl, self._rows_cache_dict.pop(tbl))
            self._cache_rows_count -= res
