#!/usr/bin/python3
import pulsar
from time import sleep

pulsar_url = 'pulsar://pulsar-cluster01-slb:6650'
topic = 'persistent://bigdata/data_cross/mini_send_tb'


def init_producer():
    client = pulsar.Client(pulsar_url)
    producer = client.create_producer(topic=topic)

    properties = {
        "worker_name": "",
        "partition": {
            "day": "int(ds_nodash)"
        },
        "sync_id": "hashlib.md5(file_path.encode('utf8')).hexdigest()",
        "total_parts": "",
        "cur_part": "",
        "md5_checksum": "hashlib.md5(payload).hexdigest()",
        "is_overwrite": False
    }
    for count in range(10):
        producer.send(('Test message %s' % count).encode('utf8'),
                      properties=properties)
        sleep(3)


if __name__ == '__main__':
    init_producer()
