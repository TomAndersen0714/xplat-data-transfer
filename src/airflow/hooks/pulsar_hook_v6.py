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
# @Date     : 2021/07/06

from airflow.hooks.dbapi_hook import DbApiHook
from pulsar import Client
from pulsar import ConsumerType
from typing import Dict


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
