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
# @Date     : 2021/08/04

import uuid
import uuid
from airflow.hooks.dbapi_hook import DbApiHook
from pulsar import Client
from _pulsar import ConsumerType
from typing import Dict, Optional


class PulsarHook(DbApiHook):
    conn_name_attr = 'pulsar_conn_id'
    default_conn_name = 'default_pulsar_conn_id'

    def __init__(self, pulsar_conn_id, topic, **kwargs):
        super(PulsarHook, self).__init__(pulsar_conn_id)
        self.topic = topic
        self._kwargs = kwargs
        self._producer = None
        self._consumer = None
        self._sub_name = None
        self.pulsar_client = self.get_conn()

    def get_conn(self):
        """
        Get pulsar client according to the configured connection in Airflow Web UI.
        """
        conn_id = getattr(self, self.conn_name_attr)
        connection = self.get_connection(conn_id)

        pulsar_cli = Client(
            f'pulsar://{connection.host}:{connection.port}', **self._kwargs
        )
        return pulsar_cli

    def send_msg(self, content: bytes, is_sync: bool = True,
                 properties: Dict = None, **kwargs):
        """
        Send a single message to specific topic.
        """
        self._producer = self._producer or self.pulsar_client.create_producer(self.topic)
        producer = self._producer

        if is_sync:
            producer.send(content, properties=properties)
        else:
            producer.send_async(content, properties=properties)

    def consume_msg_generator(self, sub_name, mode=ConsumerType.Shared,
                              timeout_millis=None, **kwargs):
        """
        Subscribe topic and return a generator for consuming message.
        Note: initial_position=InitialPosition.Latest
        """

        # compare with last subscription and get the consumer
        if self._sub_name != sub_name:
            if self._consumer is not None:
                self._consumer.close()
            self._sub_name = sub_name
            self._consumer = self.pulsar_client.subscribe(
                topic=self.topic, subscription_name=sub_name, consumer_type=mode)
        consumer = self._consumer

        while True:
            try:
                msg = consumer.receive(timeout_millis)
                self.log.info('Receive %s:%s message %s' %
                              (self.topic, self._sub_name, msg.message_id()))

                yield msg
                consumer.acknowledge(msg)
            except Exception as e:
                self.log.info(str(e))

    def close(self):
        """
        Send a end sign, and close the producer and consumer.
        """
        if self._producer:
            self._producer.close()
        if self._consumer:
            self._consumer.close()
        self.pulsar_client.close()

    @classmethod
    def get_ch_msg_header(
            cls, task_id, target_table, batch_id=None,
            source_table=None, clear_table=None, partition=None, cluster_name=None,
            source_platform=None, target_platform=None
    ) -> Dict:
        """
        Generate a ClickHouse message header for Pulsar.
        If 'partition' is None, message receiver will truncate table 'clear_table' on
        cluster 'cluster_name', else receiver will drop corresponding partition of 'clear_table'.

        If 'cluster_name' is None, message receiver will delete table data on the single node, else
        receiver will clear table on the cluster 'cluster_name'.
        """

        return {
            "task_id": str(task_id) if task_id else "",
            "db_type": "clickhouse",
            "batch_id": str(batch_id) if batch_id else str(uuid.uuid4()),
            "source_table": str(source_table) if source_table else "",
            "target_table": str(target_table) if target_table else "",
            "clear_table": str(clear_table) if clear_table else "",
            "partition": str(partition) if partition else "",
            "cluster_name": str(cluster_name) if cluster_name else "",
            "source_platform": str(source_platform) if source_platform else "",
            "target_platform": str(target_platform) if target_platform else "",
        }

    @classmethod
    def get_kudu_msg_header(
            cls, task_id, target_table, source_table=None, write_mode="upsert", batch_id=None,
            source_platform=None, target_platform=None
    ) -> Dict:
        """
        Generate a kudu message header for Pulsar.
        :param task_id:
        :param target_table:
        :param source_table:
        :param write_mode:
        :param batch_id:
        :param source_platform:
        :param target_platform:
        :return:
        """

        return {
            "task_id": str(task_id) if task_id else "",
            "db_type": "kudu",
            "batch_id": str(batch_id) if batch_id else str(uuid.uuid4()),
            "source_table": str(source_table) if source_table else "",
            "target_table": str(target_table) if target_table else "",
            "source_platform": str(source_platform) if source_platform else "",
            "dest_platform": str(target_platform) if target_platform else "",
            "write_mode": str(write_mode) if write_mode else "upsert"
        }
