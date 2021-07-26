#!/usr/bin/python3
import json
import sys
import time

import signal
from _pulsar import ConsumerType
from pulsar import Client
from processor.clickhouse_processor import ClickHouseProcessor
from processor.base_processor import BaseMsgProcessor


def flush_cache_to_db():
    """
    Flush the all cached data into corresponding database.
    """
    print('Flushing cached data in every database.')
    for _, processor in msg_processors.items():
        try:
            if processor: processor.flush_cache_to_db()
        except Exception as e:
            print(e)


def consume_msg_generator():
    """
    Subscribe topic and return a generator for consuming message.
    """
    pulsar_client = Client(pulsar_url)
    consumer = pulsar_client.subscribe(
        topic=topic, subscription_name=subscription, consumer_type=ConsumerType.Shared)
    while True:
        try:
            msg = consumer.receive(timeout_millis)
            print('Receive %s:%s message %s' % (topic, subscription, msg.message_id()))

            consumer.acknowledge(msg)
            yield msg
        except Exception as e:
            print("Consumer didn't receive any message in past %ds, and will flush all caches if necessary."
                  % (timeout_millis / 1000))
            flush_cache_to_db()


def consume():
    """
    Receive message from pulsar.
    """
    msg_gen = consume_msg_generator()
    start_time = time.time()

    # get and process every message
    for msg in msg_gen:
        db_name = msg.properties().get('db_type', 'default').lower()
        if db_name and isinstance(msg_processors[db_name], BaseMsgProcessor):
            try:
                msg_processors[db_name].process_msg(msg)
            except Exception as e:
                print(str(e))
        else:
            # pass the message to default msg processor
            pass

        # check the flush condition
        if time.time() - start_time >= insert_interval:
            start_time = time.time()
            flush_cache_to_db()


def read_config_file(json_path) -> dict:
    """
    Read json config file.
    """
    with open(json_path) as json_file:
        config = json.load(json_file)
    return config


def exit_signal_handler(signum, frame):
    """
    Exit signal handler.
    """
    print('Caught a exit signal:%s, consumer will flush all caches and close!'
          % signum)
    flush_cache_to_db()
    print('Consumer closed!')
    sys.exit(0)


if __name__ == '__main__':

    # check params
    if len(sys.argv) != 2:
        raise ValueError("Args number error!")

    # add exit signal handler
    signal.signal(signal.SIGINT, exit_signal_handler)
    signal.signal(signal.SIGTERM, exit_signal_handler)

    # read configs
    conf = read_config_file(sys.argv[1])
    print(conf)
    pulsar_url = conf.get('pulsar_url')
    topic = conf.get('topic')
    subscription = conf.get('subscription')
    assert pulsar_url and topic and subscription, \
        'pulsar_url, topic, subscription must be not empty!'

    insert_interval = int(conf.get('insert_interval', 60))
    insert_batch_rows = int(conf.get('insert_batch_rows', 30000))
    timeout_millis = int(conf.get('timeout_millis', 15000))
    base_path = conf.get('base_path', '/data2/tmp/xqc_sync')

    # initiate data cache and client cache
    msg_processors = dict()
    msg_processors['default'] = None
    if conf.get('ch_host') and conf.get('ch_port'):
        msg_processors['clickhouse'] = ClickHouseProcessor(
            conf.get('ch_host'), int(conf.get('ch_port')), insert_batch_rows)
    if conf.get('impala_host') and conf.get('impala_port'):
        # do nothing
        pass

    # set loggers

    # start consumer
    try:
        consume()
    except Exception as e:
        print(str(e))
