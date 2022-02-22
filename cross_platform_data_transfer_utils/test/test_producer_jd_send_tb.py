#!/usr/bin/python3
import pulsar
import signal
from uuid import uuid4
from time import sleep

pulsar_url = 'pulsar://pulsar-pro:6650'
topic = 'persistent://bigdata/data_cross/jd_send_tb'


def init_producer():
    client = pulsar.Client(pulsar_url)
    producer = client.create_producer(topic=topic)

    properties = {
        "task_id": "Test jd send tb",
    }
    for count in range(3):
        id = uuid4()
        producer.send(('Test message %s' % id).encode('utf8'),
                      properties=properties)
        print('Sending message %d' % id)
        sleep(3)


def graceful_exit_processor(signum, frame):
    print(f"signal {signum} {frame}")
    exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, graceful_exit_processor)
    signal.signal(signal.SIGTERM, graceful_exit_processor)

    init_producer()
