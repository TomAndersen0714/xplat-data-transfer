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
# @Time     : 2021/07/6

import os

from _pulsar import ConsumerType
from airflow.hooks.dbapi_hook import DbApiHook
from pulsar import Client, Message, Producer, Consumer
from typing import List, Dict


class PulsarHook(DbApiHook):
    """
    This only support transport batch-by-batch instead of continuously(i.e. Streaming), and
    produces or consumes message on localhost.
    """
    conn_name_attr = 'pulsar_conn_id'
    default_conn_name = 'default_pulsar_conn_id'
    __END_SIGN__ = b'END_SIGN'

    def __init__(self, *args, **kwargs):
        super(PulsarHook, self).__init__(*args, **kwargs)
        self.pulsar_client = self.get_conn()
        self._produce_topic = None
        self._consume_topic = None
        self._producer = None
        self._consumer = None

    def get_conn(self):
        """
        Get pulsar client according to the configured connection in Airflow Web UI.
        """
        conn_id = getattr(self, self.conn_name_attr)
        connection = self.get_connection(conn_id)

        pulsar_cli = Client(
            f'pulsar://{connection.host}:{connection.port}'
        )
        return pulsar_cli

    def send_msgs(self, topic, contents: List[str], code='utf-8', is_sync=False, **kwargs):
        """
        Send several messages and end sign to specific topic.
        """
        producer = self.check_producer(topic, **kwargs)

        if is_sync:
            for content in contents:
                producer.send(content.encode(code), **kwargs)
        else:
            for content in contents:
                producer.send_async(content.encode(code), **kwargs)

    def send_files(
            self, file_paths: List[str], topic,
            properties: Dict = None, batch_size=4 * 1024 * 1024,
            **kwargs
    ):
        """
        Send files to specific topic synchronously and batch-by-batch
        Producer will create specific topic automatically if it doesn't exist when producer
        send messages.

        batch_size: batch size cannot exceed the max message size(default 5 MB), max
        message size can be adjusted at server side since 2.4.0 release.
        """

        # create normal producer
        pulsar_producer = self.pulsar_client.create_producer(topic=topic)
        properties = properties or {}

        # read and sent file batch by batch
        for file_path in file_paths:
            with open(file_path, 'rb') as file:
                properties['file'] = os.path.split(file_path)[-1]

                self.log.info(f"Sending file {file_path} to {topic}")
                content = file.read(batch_size)

                while content:
                    pulsar_producer.send(content, properties=properties, **kwargs)
                    content = file.read(batch_size)

        # free resource
        pulsar_producer.send(PulsarHook.__END_SIGN__)
        pulsar_producer.close()

    def send_dir(
            self, dir_path, topic,
            properties: Dict = None, recursion=False, suffix_filter='.parquet',
            **kwargs
    ):
        """
        Send files with specific suffix in input directory synchronously
        """
        file_list = []

        if recursion:
            # traversal recursively(depth-first? no check)
            for parent_dir, __, filenames in os.walk(dir_path):
                for filename in filenames:
                    file_abs_path = os.path.join(parent_dir, filename)

                    if os.path.splitext(file_abs_path)[1] == suffix_filter:
                        file_list.append(file_abs_path)

        else:
            # just send the files in root directory
            for parent_dir, __, filenames in os.walk(dir_path):
                for filename in filenames:
                    file_abs_path = os.path.join(parent_dir, filename)

                    if os.path.splitext(file_abs_path)[-1] == suffix_filter:
                        file_list.append(file_abs_path)
                break

        self.send_files(file_list, topic, properties=properties, **kwargs)

    # context of save_as_local_file method
    from _io import _IOBase
    __last_file_path = ''
    __last_file_stream: _IOBase = None

    @staticmethod
    def save_as_local_file(msg: Message, base_dir: str = '/tmp'):
        """
        Process and append message into corresponding local file
        """

        # parse the message, if receive end sign, return __END_SIGN__
        content_b = msg.data()
        if content_b == PulsarHook.__END_SIGN__:
            return PulsarHook.__END_SIGN__

        # process current message and write to the corresponding file
        properties = msg.properties()
        file_name = properties['file']
        file_abs_path = os.path.join(base_dir, file_name)

        # open the file and append the content(for idempotent)
        if file_abs_path == PulsarHook.__last_file_path:
            file = PulsarHook.__last_file_stream
        else:
            # check the base dir
            if not os.path.exists(base_dir):
                os.mkdir(base_dir)

            # record the context
            PulsarHook.__last_file_path = file_abs_path
            PulsarHook.__last_file_stream.close()
            PulsarHook.__last_file_stream = file = open(file_abs_path, mode='ab')

        file.write(content_b)

    def consume(
            self, topic, sub_name,
            mode=ConsumerType.Exclusive, timeout_millis=None,
            msg_processor=save_as_local_file, processor_arg: Dict = None,
            **kwargs
    ):
        """
        Subscribe specific topic, and save the messages
        Consumer will automatically create topic which does not exist yet when consumer
        is going to consume message.
        """

        # create consumer
        pulsar_consumer = self.pulsar_client.subscribe(
            topic=topic, subscription_name=sub_name, consumer_type=mode,
            **kwargs
        )

        # receive and process every message.
        # Although the Java/Python API support producing batch of messages, only Java API
        # support BatchReceive in consumer methods, and we can only process message
        # one after another at present(2.8.0).
        while True:
            msg = pulsar_consumer.receive(None if timeout_millis is None else timeout_millis)

            try:
                if callable(msg_processor):
                    processor_arg = processor_arg or {}
                    if msg_processor(msg, **processor_arg) == PulsarHook.__END_SIGN__:
                        break
                else:
                    self.log.info(
                        "Receive message: '%s', id='%s'",
                        msg.data().decode('utf-8'), msg.message_id()
                    )
                pulsar_consumer.acknowledge(msg)
            except Exception as e:
                self.log.info(str(e))
                pulsar_consumer.negative_acknowledge(msg)
                break

        # free resources
        pulsar_consumer.unsubscribe()
        pulsar_consumer.close()

    def check_producer(self, topic, **kwargs):
        if self._produce_topic == topic:
            return self._producer
        else:
            if self._producer is not None:
                self._produce_topic = topic
                self._producer.close()

            self._producer = self.pulsar_client.create_producer(topic, **kwargs)
            return self._producer

    def close(self):
        """Close the client"""
        if self._producer is not None:
            self._producer.send(PulsarHook.__END_SIGN__)
            self._producer.close()

        if self._consumer is not None:
            self._consumer.unsubscribe()
            self._consumer.close()

        self.pulsar_client.close()
