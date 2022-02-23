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
# @Time     : 2021/06/30

import hashlib
import json
import os

from airflow.hooks.dbapi_hook import DbApiHook
from math import ceil
from pulsar import Client


class PulsarHook(DbApiHook):
    conn_name_attr = 'pulsar_conn_id'
    default_conn_name = 'pulsar_default_conn_id'

    def __init__(self, *args, **kwargs):
        super(PulsarHook, self).__init__(*args, **kwargs)
        self.pulsar_client = self.get_conn()
        self.worker_name = kwargs.get('worker_name', None)
        self.batch_size = kwargs.get('batch_size', 1024 * 1024)

    # Get pulsar client according to the configured connection in Airflow UI
    def get_conn(self):
        conn_id = getattr(self, self.conn_name_attr)
        connection = self.get_connection(conn_id)

        pulsar_cli = Client(
            f'pulsar://{connection.host}:{connection.port}'
        )
        return pulsar_cli

    # Send file to specific topic synchronously and batch-by-batch
    # PS: Producing a message to a topic that does not exist will automatically create
    # that topic for you as well.
    def send_file(
            self, file_path, topic,
            header=None, header_body_sep=b'',
            **kwargs
    ):
        size = os.path.getsize(file_path)
        total_batch = ceil(size / self.batch_size)

        # default header of message
        if header is None:
            header = {
                "file": file_path,
                "topic": topic
            }

        # create synchronous producer
        pulsar_producer = self.pulsar_client.create_producer(topic=topic, batching_enabled=False)

        # read and sent file batch by batch
        with open(file_path, 'rb') as file:
            header["total_batch"] = total_batch
            self.log.info(json.dumps(header, indent=2))

            for cur_batch in range(0, total_batch):
                self.log.info(f"Sending file {file_path} {cur_batch + 1} / {total_batch} ...")
                batch = file.read(self.batch_size)

                header['cur_batch'] = cur_batch
                header['md5_checksum'] = hashlib.md5(batch).hexdigest()

                header = json.dumps(header).encode(encoding='utf8')
                pulsar_producer.send(header + header_body_sep + batch, **kwargs)

        # free resource
        pulsar_producer.close()

    # Send files with specific suffix in directory synchronously
    def send_files(
            self, dir_path, topic, partition,
            header=None, recursion=False, suffix_filter='.parquet',
            **kwargs
    ):
        if recursion:
            # traversal(depth-first? no check)
            for parent_dir, __, filenames in os.walk(dir_path):
                for filename in filenames:
                    file_path = os.path.join(parent_dir, filename)

                    if os.path.splitext(file_path)[1] == suffix_filter:
                        self.send_file(file_path, topic, header, **kwargs)
        else:
            # just send the files in root directory
            for parent_dir, __, filenames in os.walk(dir_path):
                for filename in filenames:
                    file_path = os.path.join(parent_dir, filename)

                    if os.path.splitext(file_path)[1] == suffix_filter:
                        self.send_file(file_path, topic, header, **kwargs)
                break

    # Send file to specific pulsar topic asynchronously
    def send_file_async(self):
        pass

    # Send files with specific suffix in directory asynchronously
    def send_files_async(self):
        pass

    # Subscribe specific topic and process every message
    # PS: When you consume a message to a topic that does not yet exist, Pulsar
    # creates that topic for you automatically.
    def consume(
            self, topic, sub_name,
            msg_process_func=None, timeout_millis=None,
            **kwargs
    ):
        # create consumer
        pulsar_consumer = self.pulsar_client.subscribe(
            topic=topic, subscription_name=sub_name, **kwargs
        )

        # receive and process every message
        while True:
            try:
                if timeout_millis is None:
                    msg = pulsar_consumer.receive()
                else:
                    msg = pulsar_consumer.receive(timeout_millis)

                try:
                    if callable(msg_process_func):
                        msg_process_func(msg)
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
            except Exception as e:
                self.log.info(str(e))
                break

        # free resources
        pulsar_consumer.unsubscribe()
        pulsar_consumer.close()
