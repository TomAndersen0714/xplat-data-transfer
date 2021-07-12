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
# @Date     : 2021/07/08

import json
import pickle

from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.models import BaseOperator


class ClickHouseToPulsarOperator(BaseOperator):
    """
    Fetch data from ClickHouse to Pulsar.
    """
    ui_color = '#e08c8c'
    template_fields = ["ch_sql"]

    def __init__(self, task_id, ch_conn_id, ch_query_sql, pulsar_conn_id, topic,
                 with_column_types=False, max_msg_byte_size=4 * 1024 * 1024, cache_rows=100000,
                 *args, **kwargs):
        super(ClickHouseToPulsarOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.ch_conn_id = ch_conn_id
        self.pulsar_conn_id = pulsar_conn_id
        self.topic = topic
        self.ch_sql = ch_query_sql
        self.with_column_types = with_column_types
        self.max_msg_byte_size = max_msg_byte_size
        self.cache_rows = cache_rows
        self.ch_hook = ClickHouseHook(self.ch_conn_id)
        self.pulsar_hook = PulsarHook(self.pulsar_conn_id, self.topic)

    def execute(self, context):
        """
        Execute specific sql and send the result to pulsar.
        """
        self.log.info(f'Sending messages to {self.pulsar_conn_id}')
        msg_bytes_cache: bytearray = bytearray('[', encoding='utf8')
        for row_tuple in self.ch_res_tuple_generator():

            self.log.info('*' * 20)
            self.log.info(row_tuple)
            self.log.info('*' * 20)

            row_bytes = json.dumps(row_tuple).encode('utf8')

            # flush the cache and send it to pulsar when it's size reach the threshold
            if (len(msg_bytes_cache) + len(row_bytes) + 1) >= self.max_msg_byte_size:
                msg_bytes_cache[-1] = ord(']')
                self.pulsar_hook.send_msg(bytes(msg_bytes_cache))

                self.log.info('*' * 20)
                self.log.info(msg_bytes_cache.decode(encoding='utf8'))
                self.log.info('*' * 20)

                msg_bytes_cache.clear()
                msg_bytes_cache += b'['
            msg_bytes_cache += row_bytes
            msg_bytes_cache += b','

        # clear the cache if necessary
        if len(msg_bytes_cache) != 0:
            msg_bytes_cache[-1] = ord(']')
            self.pulsar_hook.send_msg(bytes(msg_bytes_cache))

            self.log.info('*' * 20)
            self.log.info(msg_bytes_cache.decode(encoding='utf8'))
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
        self.log.info(f'Executing query on {self.ch_conn_id}')
        self.log.info(f'{self.ch_sql}')
        return self.ch_hook.get_data_by_bath(self.cache_rows, self.ch_sql)
