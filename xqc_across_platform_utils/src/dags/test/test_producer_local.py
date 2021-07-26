#!/usr/bin/python3

import pulsar
from typing import List

pulsar_url = 'pulsar://10.0.2.1:6650'
topic = 'my-topic'
dest_tbl = 'tmp.truncate_test_all_1'
header = {
    'task_id': 'test',
    "db_type": "clickhouse",
    "target_table": dest_tbl,
    "partition": "{{ds_nodash}}"
}


def init_produce():
    client = pulsar.Client(pulsar_url)
    producer = client.create_producer(topic)

    # send bad message test
    row_tuple = ()
    msg_bytes_list: List[bytes] = []
    producer.send()

    # send dirty data test
    producer.send()

    # send wal data test
    producer.send()


if __name__ == '__main__':
    init_produce()
