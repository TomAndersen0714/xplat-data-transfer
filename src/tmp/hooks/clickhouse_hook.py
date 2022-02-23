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

from airflow.hooks.dbapi_hook import DbApiHook
from clickhouse_driver import Client
import time


class ClickHouseHook(DbApiHook):
    conn_name_attr = 'clickhouse_conn_id'

    default_conn_name = 'default_conn_id'

    def __init__(self, *args, **kwargs):
        super(ClickHouseHook, self).__init__(*args, **kwargs)
        self.ch_client = self.get_conn()
        self.cluster_name = kwargs.get('ch_cluster_name', 'cluster_3s_2r')

    def get_conn(self):
        conn_id = getattr(self, self.conn_name_attr)
        connection = self.get_connection(conn_id)

        ch_client = Client(
            host=str(connection.host),
            port=int(connection.port)
        )
        return ch_client

    def truncate_table(self, table_name):
        self.ch_client.execute(f"truncate table {table_name} on cluster {self.cluster_name}")

    def execute(self, sql):
        rv = self.ch_client.execute(sql)
        return rv

    def get_data_by_bath(self, row_chunk, ch_sql):
        settings = {'max_block_size': row_chunk}
        rows_gen = self.ch_client.execute_iter(
            ch_sql, settings=settings)
        return rows_gen

    def drop_partition(self, table, partition):
        sql = f"alter table {table} on cluster {self.cluster_name} drop partition {partition}"
        self.ch_client.execute(sql)

    def insert_data_with_dict_list(self, table_name, dict_list):
        # execute('INSERT INTO tmp.truncate_test_all (a,b) VALUES', [{'a': 3, 'b': 'Tom'}])
        columns = tuple(dict_list)

        sql = f"insert into {table_name} {columns} values "
        rv = self.ch_client.execute(sql, dict_list)

        print('insert into table %s: %d rows' % (table_name, rv))
        return rv

    def insert_data_with_tuple_list(self, table_name, tuple_list):
        # execute('INSERT INTO tmp.truncate_test_all (a,b) VALUES', [(1,'Tom')])
        sql = f"insert into {table_name} values "
        rv = self.ch_client.execute(sql, tuple_list)

        print('insert into table %s: %d rows' % (table_name, rv))
        return rv

    def get_fields(self, table_name):
        return self.execute(f"desc {table_name}")

    def get_records(self, sql, parameters=None):
        return self.execute(sql)
