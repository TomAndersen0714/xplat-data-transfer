#!/usr/bin/python3
import pulsar
from time import sleep

from _pulsar import ConsumerType

pulsar_url = 'pulsar://pulsar-cluster01-slb:6650'
topic = 'persistent://bigdata/data_cross/ks_send_tb'
subscription = 'bigdata_data_sync'


def init_consumer():
    client = pulsar.Client(pulsar_url)
    consumer = client.subscribe(topic, subscription, consumer_type=ConsumerType.Shared)
    while True:
        try:
            msg = consumer.receive(timeout_millis=10000)
            print(msg.message_id())
            if msg.properties():
                print(msg.properties())
            consumer.acknowledge(msg)
            sleep(3)
        except Exception as e:
            print(e)
            break


if __name__ == '__main__':
    init_consumer()
