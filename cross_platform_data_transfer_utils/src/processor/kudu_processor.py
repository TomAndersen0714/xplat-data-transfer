import logging
import pickle
import traceback
import kudu

from time import sleep

from kudu.errors import KuduBadStatus, KuduException
from pulsar import Message
from typing import Dict, List
from log_utils.log_types import *
from .base_processor import BaseMsgProcessor


class KuduProcessor(BaseMsgProcessor):

    def __init__(
            self, kudu_host='localhost', kudu_port=7051,
            insert_batch_rows=10000, logger=logging.root,
            *args, **kwargs
    ):
        BaseMsgProcessor.__init__(
            self, insert_batch_rows=insert_batch_rows, logger=logger, name='impala',
            *args, **kwargs
        )
        self.kudu_rpc_host = kudu_host
        self.kudu_rpc_port = kudu_port
        self.kudu_client = kudu.connect(
            self.kudu_rpc_host, self.kudu_rpc_port
        )

    def process_msg(self, msg: Message):
        """ Process every message. """

        # parse the message
        content = msg.data()
        properties = msg.properties()
        msg_id = msg.message_id()
        topic = msg.topic_name()
        target_table = properties.get('target_table', None)
        # self.logger.info(f'Message: {msg_id} is being processed.', log_type=NORMAL_LOG)

        # deserialize the data from message
        if target_table:
            # if target table does not exist
            if not self.kudu_client.table_exists(target_table):
                self.logger.error(
                    f"Target table '{target_table}' does not exist! \n" +
                    str(topic) + ' - ' + str(msg_id) + ' - ' + str(properties),
                    log_type=NORMAL_LOG
                )
                return

            try:
                rows_bytes_list = pickle.loads(content)
                msg_rows_list = [pickle.loads(rows_bytes_list[i])
                                 for i in range(len(rows_bytes_list))]
            except Exception as e:
                # if pickle module cannot deserialize the message, dump it to bad_message log
                # and process next message
                self.logger.error(
                    str(topic) + ' - ' + str(msg_id) + ' - ' + str(properties) + '\n' + traceback.format_exc(),
                    log_type=NORMAL_LOG)
                self.logger.error(
                    str(topic) + ' - ' + str(msg_id) + ' - ' + str(properties),
                    log_type=BAD_MSG_LOG)
                return

            # if pickle module deserialize message successfully, dump it to wal log
            # self.logger.info(str(properties) + ': ' + str(msg_rows_list), log_type=WAL_LOG)
            # self.logger.info(f"{properties}: {msg_rows_list}", log_type=WAL_LOG)

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
            self.logger.error('Target table is not specified in properties! ' +
                              str(topic) + ' - ' + str(msg_id) + ' - ' + str(properties),
                              log_type=NORMAL_LOG)
            self.logger.error(str(topic) + ' - ' + str(msg_id) + ' - ' + str(properties),
                              log_type=BAD_MSG_LOG)

        # self.logger.info(f'Message: {msg_id} processing completed.', log_type=NORMAL_LOG)

    def flush_cache_to_db(self):
        """ Flush cached data into database, only support upsert operation now. """

        table_names = list(self._rows_cache_dict.keys())
        if len(table_names) == 0:
            return

        for tbl in table_names:
            rows = self._rows_cache_dict.pop(tbl)
            res = self.upsert_data_with_dict_list(tbl, rows)
            self._cache_rows_count -= res

    def upsert_data_with_dict_list(self, table_name, records: Dict) -> int:
        """ Upsert data into specified table. """

        if not records:
            return 0
        if not self.kudu_client.table_exists(table_name):
            self.logger.error(f"Upsert failed: table {table_name} does not exist!")
            return len(records)

        # open specified table and add all write operation into kudu session
        kudu_session = self.kudu_client.new_session(flush_mode='manual')
        kudu_table = self.kudu_client.table(table_name)
        count = len(records)
        cursor = 0
        try:
            # self.logger.info(f"Upserting into {table_name}")

            # flush all cached write operation into database batch by batch
            for record in records:
                if cursor < self.insert_batch_rows:
                    kudu_session.apply(kudu_table.new_upsert(record))
                    cursor += 1
                else:
                    kudu_session.flush()
                    self.logger.info(f"Upsert into table {table_name}: {cursor} rows",
                                     log_type=NORMAL_LOG)
                    cursor = 0
                    sleep(0.1)
            if cursor != 0:
                kudu_session.flush()
                self.logger.info(f"Upsert into table {table_name}: {cursor} rows",
                                 log_type=NORMAL_LOG)
        except KuduException as e:
            self.logger.error(f"Upsert failed: {table_name}!\n{traceback.format_exc()}",
                              log_type=NORMAL_LOG)
        finally:
            return count

    def say_hello(self):
        """ Test the connection of processor. """
        # cause kudu connection is not lazy, do nothing here
        pass
