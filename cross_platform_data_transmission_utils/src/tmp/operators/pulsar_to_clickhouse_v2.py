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

import pickle

from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.models import BaseOperator


class PulsarToClickHouseOperator(BaseOperator):
    """
    Fetch data from Pulsar to ClickHouse.
    """

    def __init__(self, task_id, ch_conn_id, pulsar_conn_id, topic, subscription, dest_table,
                 with_column_types=False, batch_rows=100000,
                 *args, **kwargs):
        super(PulsarToClickHouseOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.ch_conn_id = ch_conn_id
        self.pulsar_conn_id = pulsar_conn_id
        self.topic = topic
        self.sub_name = subscription
        self.dest_table = dest_table
        self.with_column_types = with_column_types
        self.batch_rows = batch_rows
        self._kwargs = kwargs
        self.ch_hook = ClickHouseHook(self.ch_conn_id)
        self.pulsar_hook = PulsarHook(self.pulsar_conn_id, self.topic)

    def execute(self, context):
        """
        Consume pulsar message and insert into ClickHouse.
        """
        cache_rows_list = []
        msg_gen = self.pulsar_hook.consume_msg_generator(self.sub_name)

        for msg in msg_gen:
            # get message data
            content = msg.data()

            # deserialize the rows from bytes into list of tuple
            rows_bytes_list = pickle.loads(content)
            msg_rows_list = [pickle.loads(rows_bytes_list[i])
                             for i in range(len(rows_bytes_list))]

            # if cache is full, flush the cache and insert data into ClickHouse
            if len(cache_rows_list) + len(msg_rows_list) >= self.batch_rows:
                self.log.info('Inserting %s rows to ClickHouse: %s.'
                              % (len(cache_rows_list), self.dest_table))
                self.ch_hook.insert_data_with_dict_list(self.dest_table, cache_rows_list)
                cache_rows_list.clear()

            # filling the cache with rows
            cache_rows_list += msg_rows_list

        # clear the cached rows
        if len(cache_rows_list) != 0:
            self.log.info('Inserting %s rows to ClickHouse: %s.'
                          % (len(cache_rows_list), self.dest_table))
            self.ch_hook.insert_data_with_dict_list(self.dest_table, cache_rows_list)
        self.pulsar_hook.close()
