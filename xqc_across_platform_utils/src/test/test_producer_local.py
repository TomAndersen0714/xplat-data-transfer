#!/usr/bin/python3

import pickle
import pulsar

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
    # row_tuple = (1, 2)
    # row_bytes = pickle.dumps(row_tuple)
    # header['content'] = str(row_tuple)
    # producer.send(row_bytes, properties=header)

    # # send dirty data that cannot insert into corresponding table
    # row_tuple = ('Tom', 'Andersen')
    # row_bytes = pickle.dumps(row_tuple)
    # msg_rows_list = [row_bytes]
    # header['content'] = str([row_tuple])
    # producer.send(pickle.dumps(msg_rows_list), properties=header)
    #
    # send wal data test
    row_tuple = (1, 'Andersen')
    row_bytes = pickle.dumps(row_tuple)
    msg_rows_list = [row_bytes]
    header['content'] = str([row_tuple])
    producer.send(pickle.dumps(msg_rows_list), properties=header)


if __name__ == '__main__':
    init_produce()
