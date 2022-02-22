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
# @Date     : 2021/08/03

import pickle

from airflow.contrib.hooks.impala_hook import ImpalaHook
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.models import BaseOperator
from typing import List, Dict, Optional
from impala.hiveserver2 import HiveServer2Cursor


class ImpalaToPulsarOperator(BaseOperator):
    """ Fetch data from Impala to Pulsar. """

    ui_color = '#e08c8c'
    template_fields = ["imp_sql", "header"]

    def __init__(
            self,
            task_id, imp_conn_id, pulsar_conn_id, topic, header: Optional[Dict[str, str]],
            src_table=None, imp_sql=None, with_column_types=False,
            batch_rows=100000, row_mapper=None,
            *args, **kwargs
    ):
        BaseOperator.__init__(self, task_id=task_id, *args, **kwargs)
        self.imp_conn_id = imp_conn_id
        self.pulsar_conn_id = pulsar_conn_id
        self.topic = topic
        self.src_table = src_table
        self.imp_sql = imp_sql
        self.with_column_types = with_column_types
        self.max_msg_byte_size = 64 * 1024
        # max message byte size can only be adjusted on Pulsar server side(default, 5MB).
        self.batch_rows = batch_rows
        self.header = header
        self.row_mapper = row_mapper

        assert self.header is not None, "header shouldn't be None!"
        assert self.imp_sql or self.src_table, "imp_sql and imp_src_table cannot both be empty!"
        if self.imp_sql is None:
            self.imp_sql = f"SELECT * FROM {self.src_table}"
        if "task_id" not in self.header or not self.header["task_id"]:
            self.header["task_id"] = str(self.task_id)
        if "source_table" not in self.header or not self.header["source_table"]:
            self.header["source_table"] = str(self.src_table)

    def execute(self, context):
        """
        Execute specific sql and send the result to pulsar.
        """

        self.imp_cursor: HiveServer2Cursor = ImpalaHook(self.imp_conn_id).cursor
        self.pulsar_hook = PulsarHook(self.pulsar_conn_id, self.topic)

        self.log.info(f'Sending messages to {self.pulsar_conn_id}:{self.topic}')
        self.log.info(f"Message header: {self.header}")

        msg_bytes_list: List[bytes] = []
        send_rows = send_msgs = 0
        msg_rows, msg_byte_size = 0, 0

        try:
            for row_dict in self.imp_res_dict_row_generator():

                # transform every row if necessary before sending
                if self.row_mapper:
                    row_dict = self.row_mapper(row_dict)

                # serialize every row record from tuple into bytes
                row_bytes = pickle.dumps(row_dict)
                row_byte_size = len(row_bytes)

                if row_byte_size > self.max_msg_byte_size:
                    raise ValueError('The size of current row exceed the value of max_msg_byte_size!')

                # flush the cache and send it to pulsar when it's size reach the threshold
                if msg_byte_size + row_byte_size >= self.max_msg_byte_size:
                    self.log.info('*' * 20)
                    self.log.info('Sending %d rows, %d bytes message.' %
                                  (msg_rows, msg_byte_size))
                    self.log.info('*' * 20)

                    # serialize the entire list of bytes into bytes and send it to Pulsar
                    self.header["rows"] = str(len(msg_bytes_list))
                    self.pulsar_hook.send_msg(pickle.dumps(msg_bytes_list), properties=self.header)
                    msg_bytes_list.clear()
                    send_msgs += 1
                    msg_rows, msg_byte_size = 0, 0

                msg_bytes_list.append(row_bytes)
                msg_byte_size += row_byte_size
                send_rows += 1
                msg_rows += 1

            # clear the cache if necessary
            if msg_rows != 0:
                self.log.info('*' * 20)
                self.log.info('Sending %d rows, %d bytes message.' %
                              (msg_rows, msg_byte_size))
                self.log.info('*' * 20)

                self.header["rows"] = str(len(msg_bytes_list))
                self.pulsar_hook.send_msg(pickle.dumps(msg_bytes_list), properties=self.header)
                send_msgs += 1

            self.log.info('*' * 20)
            self.log.info(f"Total sent rows: {send_rows} , messages: {send_msgs}")
            self.log.info('*' * 20)
        finally:
            self.pulsar_hook.close()
            self.imp_cursor.close()

    def imp_res_dict_row_generator(self):
        self.log.info(f"Executing query on {self.imp_conn_id}：{self.imp_sql}")
        self.imp_cursor.execute(self.imp_sql)
        columns = [col[0] for col in self.imp_cursor.description]
        self.log.info(f"columns: {columns}")
        while True:
            batch = [dict(zip(columns, row)) for row in self.imp_cursor.fetchmany(self.batch_rows)]
            if batch:
                for row in batch:
                    yield row
            else:
                return
