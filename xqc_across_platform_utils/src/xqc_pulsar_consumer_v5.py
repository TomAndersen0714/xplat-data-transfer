#!/usr/bin/python3
# @Author   : chengcheng@xiaoduotech.com
# @Date     : 2021/07/24

import json
import logging
import sys
import time
import signal
import log_utils
import os

from _pulsar import ConsumerType
from pulsar import Client

from processor.base_processor import BaseMsgProcessor
from processor.clickhouse_processor import ClickHouseProcessor


def flush_cache_to_db():
    """
    Flush the all cached data into corresponding database.
    """
    logging.info('Trying to flush cached data to corresponding database.')
    for _, processor in msg_processors.items():
        try:
            if processor: processor.flush_cache_to_db()
        except Exception as e:
            logging.error('\n' + str(e))


def consume_msg_generator(
        pulsar_url, topic, subscription, consumer_type=ConsumerType.Shared, timeout_millis=15000
):
    """
    Subscribe topic and return a generator for consuming message.
    """
    pulsar_client = Client(pulsar_url)
    consumer = pulsar_client.subscribe(
        topic=topic, subscription_name=subscription, consumer_type=consumer_type)
    while True:
        try:
            msg = consumer.receive(timeout_millis)
            logging.info('Receive %s:%s message %s' % (topic, subscription, msg.message_id()))

            yield msg
            consumer.acknowledge(msg)
        except Exception as e:
            logging.info("Consumer didn't receive any message in past %ds, and will try to flush all caches."
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
                logging.error('\n' + str(e))
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
    logging.info('Caught a exit signal:%s, consumer will flush all caches and close!'
                 % signum)
    flush_cache_to_db()
    logging.info('Consumer closed!')
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
    pulsar_url = conf.get('pulsar_url')
    topic = conf.get('topic')
    subscription = conf.get('subscription')
    assert pulsar_url and topic and subscription, \
        'pulsar_url, topic, subscription must be not empty!'

    insert_interval = int(conf.get('insert_interval', 60))
    insert_batch_rows = int(conf.get('insert_batch_rows', 30000))
    timeout_millis = int(conf.get('timeout_millis', 15000))
    base_path = conf.get('base_path', '/data4/tmp/log/xqc_cross_platform')

    # initialize root logger
    root_logger_base_path = os.path.join(base_path, 'sys')
    log_utils.init_root_logger(base_path=root_logger_base_path)
    logging.info('Configures: ' + str(conf))

    # initiate data cache and client cache
    msg_processors = dict()
    msg_processors['default'] = None
    if conf.get('ch_host') and conf.get('ch_port'):
        db_name = 'clickhouse'
        ch_base_path = os.path.join(base_path, db_name)
        # create logger for message processor
        logger = log_utils.get_msg_processor_logger(logger_name=db_name, base_path=ch_base_path)
        msg_processors[db_name] = ClickHouseProcessor(
            ch_host=conf.get('ch_host'), ch_port=int(conf.get('ch_port')),
            insert_batch_rows=insert_batch_rows, logger=logger
        )
    if conf.get('impala_host') and conf.get('impala_port'):
        # do nothing
        pass

    # start consumer
    try:
        consume()
    except Exception as e:
        logging.error('\n' + str(e))
