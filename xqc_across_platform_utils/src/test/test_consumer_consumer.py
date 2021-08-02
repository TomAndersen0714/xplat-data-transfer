#!/usr/bin/python3
import pulsar
from time import sleep


def init_consume():
    client = pulsar.Client('pulsar://10.0.2.1:6650')
    consumer = client.subscribe('my-topic', 'my-subscription')
    while True:
        try:
            msg = consumer.receive(5000)
            print(msg.data())
            print(msg.properties())
            consumer.acknowledge(msg)
            sleep(1)
        except Exception as e:
            consumer.close()
            client.close()
            print(e)


if __name__ == '__main__':
    init_consume()
