#!/usr/bin/python3
import logging
import pickle

from clickhouse_driver import Client
from pulsar import Message

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

    def process_msg(self, msg: Message):
        """ Process every message. """

        # parse the message
        content = msg.data()
        properties = msg.properties()
        msg_id = msg.message_id()
        topic = msg.topic_name()
        target_table = properties.get('target_table', None)
        self.logger.info(f'Processing message: {msg_id}', log_type=NORMAL_LOG)

        # deserialize the data from message
        if target_table:
            try:
                rows_bytes_list = pickle.loads(content)
                msg_rows_list = [pickle.loads(rows_bytes_list[i])
                                 for i in range(len(rows_bytes_list))]
            except Exception as e:
                # if pickle module cannot deserialize the message, dump it to bad_message log
                # and process next message
                self.logger.error(str(topic) + ' - ' + str(msg_id) + ' - ' + str(properties) + '\n' + str(e),
                                  log_type=NORMAL_LOG)
                self.logger.error(str(topic) + ' - ' + str(msg_id) + ' - ' + str(properties),
                                  log_type=BAD_MSG_LOG)
                return

            # if pickle module deserialize message successfully, dump it to wal log
            self.logger.info(str(properties) + ': ' + str(msg_rows_list), log_type=WAL_LOG)

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
            # the properties of message doesn't contain 'target_table'
            self.logger.error('Target table is not specified in properties! ' +
                              str(topic) + ' - ' + str(msg_id) + ' - ' + str(properties),
                              log_type=NORMAL_LOG)
            # dump this message to bad message log
            self.logger.error(str(topic) + ' - ' + str(msg_id) + ' - ' + str(properties),
                              log_type=BAD_MSG_LOG)
        self.logger.info(f'Message: {msg_id} processing completed.', log_type=NORMAL_LOG)

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
