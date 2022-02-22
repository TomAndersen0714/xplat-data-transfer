# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# @Author   : chengcheng@xiaoduotech.com
# @Date     : 2021/08/11

import pickle

from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.models import BaseOperator
from typing import List, Dict, Optional, Tuple
from clickhouse_driver import Client


class ClickHouseToPulsarOperator(BaseOperator):
    """ Fetch data from ClickHouse to Pulsar. """

    ui_color = '#e08c8c'
    template_fields = ["ch_sql", "header"]

    def __init__(
            self,
            task_id, ch_conn_id, ch_query_sql,
            pulsar_conn_id, topic, header: Optional[Dict[str, str]],
            src_table=None, row_mapper=None, cache_rows=100000,
            *args, **kwargs
    ):
        super(ClickHouseToPulsarOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.ch_conn_id = ch_conn_id
        self.pulsar_conn_id = pulsar_conn_id
        self.topic = topic
        self.ch_sql = ch_query_sql
        self.src_table = src_table
        self.row_mapper = row_mapper
        self.max_msg_byte_size = 64 * 1024
        # max message byte size can only be adjusted on Pulsar server side(default, 5MB).
        self.cache_rows = cache_rows
        self.header = header
        self.schemas: Optional[List[Tuple]] = None
        self.columns: Optional[List[str]] = None

        assert self.header is not None, "header shouldn't be None!"
        assert self.ch_sql or self.src_table, "ch_sql and src_table cannot both be empty!"
        if self.ch_sql is None:
            self.ch_sql = f"SELECT * FROM {self.src_table}"
        if "task_id" not in self.header or not self.header["task_id"]:
            self.header["task_id"] = str(self.task_id)
        if "source_table" not in self.header or not self.header["source_table"]:
            self.header["source_table"] = str(self.src_table)

    def execute(self, context):
        """
        Execute specific sql and send the result to pulsar.
        """

        self.ch_client: Client = ClickHouseHook(self.ch_conn_id).ch_client
        self.pulsar_hook = PulsarHook(self.pulsar_conn_id, self.topic)

        self.log.info(f'Sending messages to {self.pulsar_conn_id} - {self.topic}')
        self.log.info(f"Message header: {self.header}")

        msg_bytes_list: List[bytes] = []
        send_rows = send_msgs = 0
        msg_row_total, msg_byte_size = 0, 0

        try:
            for row_tuple in self.ch_res_row_generator():
                send_rows += 1

                # add column name to every row tuple
                row_dict = dict(zip(self.columns, row_tuple))
                if self.row_mapper:
                    row_dict = self.row_mapper(row_dict)

                # serialize every row record from dict into bytes
                row_bytes = pickle.dumps(row_dict)
                row_byte_size = len(row_bytes)

                if row_byte_size > self.max_msg_byte_size:
                    self.log.error(row_dict)
                    raise ValueError('The size of current row exceed the value of max_msg_byte_size!')

                # flush the cache and send it to pulsar when it's size reach the threshold
                if msg_byte_size + row_byte_size >= self.max_msg_byte_size:
                    self.log.info('*' * 20)
                    self.log.info('Sending %d rows, %d bytes message.' %
                                  (msg_row_total, msg_byte_size))
                    self.log.info('*' * 20)

                    # serialize the entire list of bytes into bytes and send it to Pulsar
                    self.pulsar_hook.send_msg(pickle.dumps(msg_bytes_list), properties=self.header)
                    msg_bytes_list.clear()
                    send_msgs += 1
                    msg_row_total, msg_byte_size = 0, 0

                msg_bytes_list.append(row_bytes)
                msg_row_total += 1
                msg_byte_size += row_byte_size

            # clear the cache if necessary
            if msg_row_total != 0:
                self.pulsar_hook.send_msg(pickle.dumps(msg_bytes_list), properties=self.header)

                self.log.info('*' * 20)
                self.log.info('Sending %d rows, %d bytes message.' %
                              (msg_row_total, msg_byte_size))
                self.log.info('*' * 20)
                send_msgs += 1

            self.log.info('*' * 20)
            self.log.info(f"Total sent rows: {send_rows} , messages: {send_msgs}")
            self.log.info('*' * 20)
        finally:
            self.ch_client.disconnect()
            self.pulsar_hook.close()

    def ch_res_row_generator(self):
        """
        Execute the sql query, fetch the result batch-by-batch, and return the generator as a cursor.
        Every row of result is represented by a tuple(e.g. (1,'Tom','Andersen'))
        You can also query the column names and types by switch the param 'self.with_column_types' to
        true.(...)
        """
        # return self.ch_hook.get_data_by_bath(self.cache_rows, self.ch_sql, self.with_column_types)

        self.log.info(f'Executing query on {self.ch_conn_id}：{self.ch_sql}')
        settings = {'max_block_size': self.cache_rows}
        gen = self.ch_client.execute_iter(self.ch_sql, with_column_types=True, settings=settings)
        self.schemas = next(gen)
        self.columns = [x[0] for x in self.schemas]
        self.log.info(f"schemas: {self.schemas}")
        self.log.info(f"columns: {self.columns}")
        return gen
