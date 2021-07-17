#!/usr/bin/python3
import pulsar

import signal
from time import sleep

pulsar_url = 'pulsar://pulsar-pro:6650'
topic = 'persistent://bigdata/data_cross/jd_send_tb'


def init_producer():
    client = pulsar.Client(pulsar_url)
    producer = client.create_producer(topic=topic)

    properties = {
        "task_id": "Test jd send tb",
    }
    for count in range(10):
        producer.send(('Test message %s' % count).encode('utf8'),
                      properties=properties)
        print('Sending message %d' % count)
        sleep(3)


def quit(signum, frame):
    print(f"signal {signum} {frame}")
    pass


if __name__ == '__main__':
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)

    init_producer()
