#!/usr/bin/python3
# @Author   : chengcheng@xiaoduotech.com
# @Date     : 2021/07/30

import json
import logging
import sys
import threading
import time
import signal
import log_utils
import os

from typing import Dict
from _pulsar import ConsumerType, Timeout
from pulsar import Client

from processor.base_processor import BaseMsgProcessor
from processor.clickhouse_processor_v1 import ClickHouseProcessor


def flush_cache_to_db(msg_processors):
    """
    Flush the all cached data into corresponding database.
    """
    # logging.info('Trying to flush all cached data to corresponding database.')
    for _, processor in msg_processors.items():
        try:
            if processor:
                processor.flush_cache_to_db()
        except Exception as e:
            logging.error(str(e))


def consume_msg_generator(
        pulsar_url, topic, subscription, msg_processors, stop_signal: threading.Event,
        consumer_type=ConsumerType.Shared, timeout_millis=15000,
):
    """
    Subscribe topic and return a generator for consuming message.
    """
    try:
        pulsar_client = Client(pulsar_url)
        consumer = pulsar_client.subscribe(
            topic=topic, subscription_name=subscription, consumer_type=consumer_type)
        logging.info(f"Subscribing to [{pulsar_url}, {topic}, {subscription}], "
                     f"consumer type: {consumer_type}.")
    except Exception as e:
        # if occur a unexpected error
        logging.error('\n' + str(e))
        return ''

    while True:
        # check the stop signal
        if stop_signal.is_set():
            # if the generator is closed
            logging.info(f"Consumer of [{pulsar_url}, {topic}, {subscription}] is closing!")
            consumer.close()
            pulsar_client.close()
            flush_cache_to_db(msg_processors)
            return
        try:
            msg = consumer.receive(timeout_millis)
            logging.info(f'Receive [%s, %s, %s], message: %s' %
                         (pulsar_url, topic, subscription, msg.message_id()))

            yield msg
            consumer.acknowledge(msg)
        except Timeout:
            # if the consumer didn't get any message
            # logging.info("Consumer didn't receive any message in past %ds, and will try to flush all caches."
            #              % (timeout_millis / 1000))
            flush_cache_to_db(msg_processors)
        except Exception as e:
            # if occur a unexpected error
            logging.error('\n' + str(e))
            flush_cache_to_db(msg_processors)


def consume(
        pulsar_url, topic, subscription, consumer_type, timeout_millis,
        msg_processors, stop_signal: threading.Event
):
    """
    Consume message from pulsar.
    """
    msg_gen = consume_msg_generator(
        pulsar_url, topic, subscription,
        msg_processors=msg_processors, consumer_type=consumer_type,
        timeout_millis=timeout_millis, stop_signal=stop_signal
    )
    start_time = time.time()

    # get and process every message
    for msg in msg_gen:

        db_name = msg.properties().get('db_type', 'default').lower()
        if db_name and isinstance(msg_processors[db_name], BaseMsgProcessor):
            try:
                msg_processors[db_name].process_msg(msg)
            except Exception as e:
                logging.error('\n' + str(e))
        else:
            # pass the message to default msg processor
            pass

        # check the flush condition
        if time.time() - start_time >= insert_interval:
            start_time = time.time()
            flush_cache_to_db(msg_processors)


def read_config_file(json_path) -> dict:
    """
    Read json config file.
    """
    with open(json_path) as json_file:
        config = json.load(json_file)
    return config


def get_msg_processors(conf: dict) -> Dict[str, BaseMsgProcessor]:
    """
    Create the corresponding message processor based on the configures.
    """
    msg_processors = dict()
    msg_processors['default'] = None

    # set processor for clickhouse message
    if conf.get('ch_host') and conf.get('ch_port'):
        db_name = 'clickhouse'
        ch_base_path = os.path.join(base_path, db_name)
        # create logger for message processor
        logger = log_utils.get_msg_processor_logger(
            logger_name=db_name, base_path=ch_base_path
        )
        msg_processors[db_name] = ClickHouseProcessor(
            ch_host=conf.get('ch_host'), ch_port=int(conf.get('ch_port')),
            insert_batch_rows=insert_batch_rows, logger=logger
        )

    # set processor for impala message
    if conf.get('impala_host') and conf.get('impala_port'):
        pass

    return msg_processors


def exit_signal_handler(signum, frame):
    """
    Exit signal handler.
    """
    logging.info(
        'Caught a exit signal:%s, all threads will flush caches and close!' % signum
    )
    stop_signal.set()


if __name__ == '__main__':

    # check params
    if len(sys.argv) != 2:
        raise ValueError("Args number error!")

    # add exit signal handler
    signal.signal(signal.SIGINT, exit_signal_handler)
    signal.signal(signal.SIGTERM, exit_signal_handler)

    # read configs
    conf = read_config_file(sys.argv[1])
    pulsar_url: str = conf.get('pulsar_url')
    topic: str = conf.get('topic')
    subscription: str = conf.get('subscription')
    assert pulsar_url and topic and subscription, \
        'pulsar_url, topic, or subscription must be not empty!'

    insert_interval = int(conf.get('insert_interval', 60))
    insert_batch_rows = int(conf.get('insert_batch_rows', 30000))
    timeout_millis = int(conf.get('timeout_millis', 15000))
    base_path: str = conf.get('base_path', '/data2/tmp/xqc_cross_platform/log')

    # initialize root logger
    root_logger_base_path = os.path.join(base_path, 'sys')
    log_utils.init_root_logger(base_path=root_logger_base_path)
    logging.info('Configures: ' + str(conf))

    # multi threading setting
    pulsar_url = pulsar_url.strip('; ')  # strip the spaces and semicolons in the head and tail of params
    pulsar_urls = pulsar_url.split(';')
    topic = topic.strip('; ')
    topics = topic.split(';')
    subscription = subscription.strip('; ')
    subscriptions = subscription.split(';')

    assert len(pulsar_urls) and len(topics) and len(subscriptions), \
        'The number of pulsar_url, topic, and subscription must be the same!'
    threads_count = len(pulsar_urls)
    threads = []

    # deploy consumer in multi thread
    stop_signal = threading.Event()
    for i in range(threads_count):
        # create processors for every child thread
        processors = get_msg_processors(conf)
        # start current thread
        kw_params = dict(
            pulsar_url=pulsar_urls[i],
            topic=topics[i],
            subscription=subscriptions[i],
            consumer_type=ConsumerType.Shared,
            timeout_millis=timeout_millis,
            msg_processors=processors,
            stop_signal=stop_signal
        )
        t = threading.Thread(target=consume, kwargs=kw_params)
        t.start()
        threads.append(t)

    # wait for all child thread
    [threads[x].join() for x in range(0, threads_count)]
    logging.info("Main thread exit!")
