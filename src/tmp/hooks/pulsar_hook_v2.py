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

from pulsar import ConsumerType
from airflow.hooks.dbapi_hook import DbApiHook
from pulsar import Client, Message
from typing import List, Dict


# Only support transport batch-by-batch instead of continuously(Streaming)
class PulsarHook(DbApiHook):
    conn_name_attr = 'pulsar_conn_id'
    default_conn_name = 'default_pulsar_conn_id'
    __END_SIGN__ = b'END_SIGN'

    def __init__(self, *args, **kwargs):
        super(PulsarHook, self).__init__(*args, **kwargs)
        self.pulsar_client = self.get_conn()

    # Get pulsar client according to the configured connection in Airflow UI
    def get_conn(self):
        conn_id = getattr(self, self.conn_name_attr)
        connection = self.get_connection(conn_id)

        pulsar_cli = Client(
            f'pulsar://{connection.host}:{connection.port}'
        )
        return pulsar_cli

    # Send several messages and end sign to specific topic
    def send_msgs(self, topic, contents: List[str], is_sync=False, **kwargs):
        producer = self.pulsar_client.create_producer(topic, **kwargs)
        if is_sync:
            for content in contents:
                producer.send(content, **kwargs)
            producer.send(self.__END_SIGN__, **kwargs)
        else:
            for content in contents:
                producer.send_async(content, **kwargs)
            producer.send_async(self.__END_SIGN__)

    # Send files to specific topic synchronously and batch-by-batch
    # Producer will create specific topic automatically if it doesn't exist when producer
    # send messages.
    def send_files(
            self, file_paths: List[str], topic, file_type='parquet',
            properties: Dict = None, batch_size=4 * 1024 * 1024,
            **kwargs
            # batch size cannot exceed the max message size(default 5 MB), max message size can be
            # adjusted at server side since 2.4.0 release.
    ):
        # create normal producer
        pulsar_producer = self.pulsar_client.create_producer(topic=topic)
        properties = {} if properties is None else properties

        # read and sent file batch by batch
        for file_path in file_paths:
            with open(file_path, 'rb') as file:
                properties['file'] = file_path
                properties['type'] = file_type

                self.log.info(f"Sending file {file_path}.{file_type} to {topic}")
                content = file.read(batch_size)

                while content:
                    pulsar_producer.send(content, properties=properties, **kwargs)
                    content = file.read(batch_size)

        # free resource
        pulsar_producer.send(self.__END_SIGN__)
        pulsar_producer.close()

    # Send files with specific suffix in directory synchronously
    def send_dir(
            self, dir_path, topic,
            header=None, recursion=False, suffix_filter='.parquet',
            **kwargs
    ):
        file_list = []

        if recursion:
            # traversal recursively(depth-first? no check)
            for parent_dir, __, filenames in os.walk(dir_path):
                for filename in filenames:
                    file_path = os.path.join(parent_dir, filename)

                    if os.path.splitext(file_path)[1] == suffix_filter:
                        file_list.append(file_path)

        else:
            # just send the files in root directory
            for parent_dir, __, filenames in os.walk(dir_path):
                for filename in filenames:
                    file_path = os.path.join(parent_dir, filename)

                    if os.path.splitext(file_path)[-1] == suffix_filter:
                        file_list.append(file_path)
                break

        self.send_files(file_list, topic, file_type=suffix_filter, header=header, **kwargs)

    # Process and merge message into a file
    @staticmethod
    def save_as_file(msg: Message):
        # parse the message
        content_b = msg.data()
        properties = msg.properties()


        # process current message and write to the corresponding file


        # if receive __END_SIGN__, return __END_SIGN__
        pass

    # Subscribe specific topic and process all messages
    # Consumer will automatically create topic which does not exist yet when consumer
    # is going to consume message.
    def consume(
            self, topic, sub_name,
            msg_process_func=save_as_file, mode=ConsumerType.Exclusive, timeout_millis=None,
            **kwargs
    ):
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
                if callable(msg_process_func):
                    if msg_process_func(msg) == self.__END_SIGN__:
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
