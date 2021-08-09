#!/usr/bin/python3

import pickle
import pulsar
import uuid

pulsar_url = 'pulsar://10.0.2.1:6650'
topic = 'my-topic'
dest_tbl = 'tmp.drop_partition_test_all'
clear_table = 'tmp.drop_partition_test_local'

header = {
    'task_id': 'test',
    "db_type": "clickhouse",
    "target_table": dest_tbl,
    "clear_table": clear_table,
    "partition": "(2021,7,13)",
    "cluster_name": "cluster_3s_2r"
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
    batch_id = str(uuid.uuid4())
    header['partition'] = "(2021,7,13)"
    header['batch_id'] = batch_id
    row_tuple = (batch_id, 2021, 7, 13)
    row_bytes = pickle.dumps(row_tuple)
    msg_rows_list = [row_bytes]
    header['content'] = str([row_tuple])
    print(header)
    producer.send(pickle.dumps(msg_rows_list), properties=header)

    batch_id = str(uuid.uuid4())
    header['partition'] = "(2021,7,14)"
    header['batch_id'] = batch_id
    row_tuple = (batch_id, 2021, 7, 14)
    row_bytes = pickle.dumps(row_tuple)
    msg_rows_list = [row_bytes]
    header['content'] = str([row_tuple])
    print(header)
    producer.send(pickle.dumps(msg_rows_list), properties=header)

    batch_id = str(uuid.uuid4())
    header['partition'] = "(2021,7,15)"
    header['batch_id'] = batch_id
    row_tuple = (batch_id, 2021, 7, 15)
    row_bytes = pickle.dumps(row_tuple)
    msg_rows_list = [row_bytes]
    header['content'] = str([row_tuple])
    print(header)
    producer.send(pickle.dumps(msg_rows_list), properties=header)

    header['partition'] = "(2021,7,15)"
    header['batch_id'] = batch_id
    row_tuple = (str(uuid.uuid4()), 2021, 7, 15)
    row_bytes = pickle.dumps(row_tuple)
    msg_rows_list = [row_bytes]
    header['content'] = str([row_tuple])
    print(header)
    producer.send(pickle.dumps(msg_rows_list), properties=header)


if __name__ == '__main__':
    init_produce()
