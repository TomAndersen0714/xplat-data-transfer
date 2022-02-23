#!/usr/bin/python3
import signal

import pulsar
from time import sleep

from pulsar import ConsumerType

pulsar_url = 'pulsar://pulsar-pro:6650'
# pulsar_url = 'pulsar://pulsar-cluster01-slb:6650'
topic = 'persistent://bigdata/data_cross/tb_send_jd'
subscription = 'bigdata_data_sync'


def init_consumer():
    client = pulsar.Client(pulsar_url)
    consumer = client.subscribe(topic, subscription, consumer_type=ConsumerType.Shared)
    while True:
        try:
            msg = consumer.receive(timeout_millis=15000)
            print(msg.properties().get("batch_id", ""))
            # if msg.properties():
            #     print(msg.properties())
            consumer.acknowledge(msg)
            sleep(0.05)
        except Exception as e:
            print(e)
            break


def quit(signum, frame):
    print(f"signal {signum} {frame}")
    exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)

    init_consumer()
