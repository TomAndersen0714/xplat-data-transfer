#!/usr/bin/python3
import pickle

import pulsar
from _pulsar import ConsumerType
from time import sleep


def init_consume():
    client = pulsar.Client('pulsar://10.0.2.1:6650')
    consumer = client.subscribe(
        topic='my-topic',
        subscription_name='my-subscription',
        consumer_type=ConsumerType.Shared
    )
    while True:
        try:
            msg = consumer.receive(5000)
            print(msg.properties())
            # content = msg.data()
            # rows_bytes_list = pickle.loads(content)
            # msg_rows_list = [pickle.loads(rows_bytes_list[i]) for i in range(len(rows_bytes_list))]
            # print(msg_rows_list)
            consumer.acknowledge(msg)
            # sleep(3)
        except Exception as e:
            consumer.close()
            client.close()
            print(e)


if __name__ == '__main__':
    init_consume()
