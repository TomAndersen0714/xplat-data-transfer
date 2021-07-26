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
# @Date     : 2021/07/18

import pickle

from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.models import BaseOperator
from typing import List, Dict, Optional


class ClickHouseToPulsarOperator(BaseOperator):
    """
    Fetch data from ClickHouse to Pulsar.
    """
    ui_color = '#e08c8c'
    template_fields = ["ch_sql", "header"]

    def __init__(self, task_id, ch_conn_id, ch_query_sql, pulsar_conn_id, topic,
                 with_column_types=False, max_msg_byte_size=4 * 1024 * 1024, cache_rows=100000,
                 header: Optional[Dict[str, str]] = None,
                 *args, **kwargs):
        super(ClickHouseToPulsarOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.ch_conn_id = ch_conn_id
        self.pulsar_conn_id = pulsar_conn_id
        self.topic = topic
        self.ch_sql = ch_query_sql
        self.with_column_types = with_column_types
        self.max_msg_byte_size = max_msg_byte_size
        # max message byte size can only be adjusted on Pulsar server side(default, 5MB).
        self.cache_rows = cache_rows
        self._kwargs = kwargs
        self.header = header
        self.ch_hook = ClickHouseHook(self.ch_conn_id)
        self.pulsar_hook = PulsarHook(self.pulsar_conn_id, self.topic)

    def execute(self, context):
        """
        Execute specific sql and send the result to pulsar.
        """

        self.log.info(f'Sending messages to {self.pulsar_conn_id}')
        msg_bytes_list: List[bytes] = []
        msg_row_total, msg_byte_size = 0, 0

        for row_tuple in self.ch_res_tuple_generator():

            # serialize every row record from tuple into bytes
            row_bytes = pickle.dumps(row_tuple)
            row_byte_size = len(row_bytes)

            if row_byte_size > self.max_msg_byte_size:
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

        self.pulsar_hook.close()

    def ch_res_tuple_generator(self):
        """
        Execute the sql query, fetch the result batch-by-batch, and return the generator as a cursor.
        Every row of result is represented by a tuple(e.g. (1,'Tom','Andersen'))
        You can also query the column names and types by switch the param 'self.with_column_types' to
        true.(...)
        """
        # return self.ch_hook.get_data_by_bath(self.cache_rows, self.ch_sql, self.with_column_types)

        self.log.info(f'Executing query on {self.ch_conn_id}：{self.ch_sql}')
        return self.ch_hook.get_data_by_bath(self.cache_rows, self.ch_sql)
