#!/usr/bin/python3
import pulsar
from time import sleep

pulsar_url = 'pulsar://pulsar-cluster01-slb:6650'
topic = 'persistent://bigdata/data_cross/mini_send_tb'


def init_producer():
    client = pulsar.Client(pulsar_url)
    producer = client.create_producer(topic=topic)

    properties = {
        "task_id": "mini_qc_to_tb"
    }
    for count in range(3):
        print('sending test message %d' % count)
        producer.send(('Test message %s' % count).encode('utf8'), properties=properties)
        sleep(3)


if __name__ == '__main__':
    init_producer()
