#!/usr/bin/python3
import signal
from uuid import uuid4
from pulsar import Client
from time import sleep

pulsar_url = 'pulsar://pulsar-cluster01-slb:6650'
topic = 'persistent://bigdata/data_cross/yz_send_tb'


def init_producer():
    client = Client(pulsar_url)
    producer = client.create_producer(topic=topic)

    properties = {
        "task_id": "Test dy send tb",
    }
    for count in range(3):
        id = str(uuid4())
        properties["id"] = id
        producer.send(('Test message %s' % id).encode('utf8'),
                      properties=properties)
        print('Sending message %s' % id)
        sleep(3)


def graceful_exit_processor(signum, frame):
    print(f"signal {signum} {frame}")
    exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, graceful_exit_processor)
    signal.signal(signal.SIGTERM, graceful_exit_processor)

    init_producer()
