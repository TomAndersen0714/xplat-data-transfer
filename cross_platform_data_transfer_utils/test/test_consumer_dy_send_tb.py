#!/usr/bin/python3
import signal
from pulsar import Client
from time import sleep
from _pulsar import ConsumerType

pulsar_url = 'pulsar://pulsar-cluster01-slb:6650'
topic = 'persistent://bigdata/data_cross/yz_send_tb'
subscription = 'bigdata_data_sync'


def init_consumer():
    client = Client(pulsar_url)
    consumer = client.subscribe(topic, subscription, consumer_type=ConsumerType.Shared)
    while True:
        try:
            msg = consumer.receive(timeout_millis=3000)
            print(msg.message_id())
            if msg.properties():
                print(msg.properties())
            consumer.acknowledge(msg)
            sleep(1)
        except Exception as e:
            print(e)
            break


def graceful_exit_processor(signum, frame):
    print(f"signal {signum} {frame}")
    exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, graceful_exit_processor)
    signal.signal(signal.SIGTERM, graceful_exit_processor)

    init_consumer()
