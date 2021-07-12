#!/usr/bin/python3

# @Author   : chengcheng@xiaoduotech.com
# @Time     : 2021/07/06

import os

from _pulsar import ConsumerType
from pulsar import Client, Message
from typing import Dict

__last_file_path__ = ''
__file_list__ = []
__last_file_stream__ = None
__END_SIGN__ = b'END_SIGN'


def save_as_local_file(msg: Message, base_dir: str = '/tmp', **kwargs):
    """
    Process and append message into corresponding local file.
    """

    global __last_file_path__, __last_file_stream__, __file_list__, __END_SIGN__

    # parse the message, if receive end sign, return __END_SIGN__
    content_b = msg.data()
    if content_b == __END_SIGN__:
        return __END_SIGN__

    # process current message and write to the corresponding file
    properties = msg.properties()
    file_name = properties['file']
    file_abs_path = os.path.join(base_dir, file_name)

    # open the file and append the content(for idempotent)
    if file_abs_path == __last_file_path__:
        file = __last_file_stream__
    else:
        # check the base dir
        if not os.path.exists(base_dir):
            os.mkdir(base_dir)

        # record the context
        __file_list__.append(file_abs_path)
        __last_file_path__ = file_abs_path
        __last_file_stream__.close()
        __last_file_stream__ = file = open(file_abs_path, mode='ab')

    file.write(content_b)


def consume(
        pulsar_url, topic, sub_name,
        mode=ConsumerType.Exclusive, timeout_millis=None,
        msg_processor=save_as_local_file, processor_arg: Dict = None,
        **kwargs
):
    """
    Subscribe specific topic, and save the messages.
    Consumer will automatically create topic which does not exist yet when consumer
    is going to consume message.
    """

    # connect
    global __END_SIGN__
    pulsar_client = Client(pulsar_url)
    pulsar_consumer = pulsar_client.subscribe(
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
                if msg_processor(msg, **processor_arg) == __END_SIGN__:
                    break
            else:
                print(
                    "Receive message: '%s', id='%s'",
                    msg.data().decode('utf-8'), msg.message_id()
                )
            pulsar_consumer.acknowledge(msg)
        except Exception as e:
            print(str(e))
            pulsar_consumer.negative_acknowledge(msg)
            break

    # free resources
    pulsar_consumer.unsubscribe()
    pulsar_consumer.close()


def upload_parquet_to_ch(ch_container_id, ch_tcp_port, file_container_path, dest_table):
    """
    Upload the parquet file to local ClickHouse Server.
    """

    upload_cmd = f"""
    docker exec {ch_container_id} bash -c "clickhouse-client --port={ch_tcp_port} --query 
    'insert into {dest_table} format Parquet' < {file_container_path} " 
    """

    os.system(upload_cmd)


if __name__ == '__main__':
    conf = {
        'pulsar_url': 'pulsar://pulsar-cluster01-slb:6650',
        'topic': 'persistent://bigdata/xqc_cross_platform/ch_kefu_stat_data_sync',
        'subscription': 'taobao_ch_sync',
        'ch_container_id': '9e639bbee57d',
        'ch_tcp_port': 29000,
        'base_dir': '/data0/clickhouse/data/tmp/data/tmp'
    }

    # Consume message and merge them into file
    consume(conf['pulsar_url'], conf['topic'], conf['subscription'], base_dir=conf['base_dir'])

    # upload the local file to local ClickHouse Server
    upload_parquet_to_ch(conf['ch_container_id'], conf['ch_tcp_port'], conf[''])
