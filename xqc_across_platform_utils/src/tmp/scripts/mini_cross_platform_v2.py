import hashlib
import json
import logging
import os
import shutil
import pulsar

from logging import handlers
from subprocess import call

# @Author   : chengcheng@xiaoduotech.com
# @Time     : 2021/06/30

# Log handler
log_handler = handlers.RotatingFileHandler("./data_sync.log", maxBytes=1024 * 1024 * 100)
log_handler.setLevel(logging.DEBUG)
logger = logging.getLogger("root")
logger.addHandler(log_handler)

# Impala SQL
hdfs_tmp_dir = "/user/xd_stat/data_sync"
add_partition_imp_sql = "alter table {table} add if not exists {partition_clause}"
load_data_overwrite_imp_sql = "load data inpath '{data_dir}' overwrite into table {table} {partition_clause};"
load_data_into_imp_sql = "load data inpath '{data_dir}' into table {table} {partition_clause};"
IMPALA_RESERVE_WORDS = ('date', 'data', 'no',)

# ClickHouse connection
ch_host = 'zjk-bigdata006'
ch_tcp_port = '29000'
ch_container_id = '9e639bbee57d'
ch_host_sync_dir = '/data0/clickhouse/data/tmp/data/ods'
ch_container_sync_dir = '/var/lib/clickhouse/tmp/data/ods'
cluster_name = 'cluster_3s_2r'

# Cache for all received batches
local_cache = {}


# Add single quote to the beginning and end of string
def str_fmt(s):
    return "'%s'" % s


# Generate partition condition string and add '`' symbol for reserved words of impala
# (e.g. "`date`=20190918","`date`='20190918'")
def get_par_cond_eqa(partition):
    return ','.join(
        [f"{'`%s`' % k if k in IMPALA_RESERVE_WORDS else k}={str_fmt(v) if isinstance(v, str) else v}"
         for k, v in partition.items()]
    )


# Execute specific commands
def _call(cmd_list):
    print(f"Executing {cmd_list}")
    return call(cmd_list)


# Compact all batches of same file into one, and return the dest file path
def compact_file(worker_name, cur_id, data_list):
    print(f"Try to load data {worker_name} {cur_id} to ClickHouse cluster ...")

    data_sync_dir = f'{ch_host_sync_dir}/{worker_name}'
    file_path = f'{data_sync_dir}/{worker_name}_{cur_id}.parq'

    # clear dest directory
    try:
        shutil.rmtree(data_sync_dir)
        os.makedirs(data_sync_dir, exist_ok=True)
    except Exception as e:
        print(str(e))
        exit(1)

    with open(file_path, 'ab') as f:
        for d in data_list:
            f.write(d)

    return file_path


# Insert data into ClickHouse temporary table, and flush destination table in ClickHouse
def flush_data_to_ch(file_abs_path, tmp_table, dest_table):
    from clickhouse_driver import Client
    ch_client = Client(host=ch_host, port=ch_tcp_port)

    # truncate tmp table
    ch_sql = f"truncate table {tmp_table} on cluster {cluster_name}"
    ch_client.execute(ch_sql)

    # insert data file into ClickHouse tmp table
    # this cmd must be executed on zjk-bigdata006/zjk-bigdata007/zjk-bigdata008
    ch_shell_cmd = f"""docker exec {ch_container_id} bash -c 'clickhouse-client --port=19000 --query 
    "insert into {tmp_table} format Parquet" < {file_abs_path}'"""

    print(f"Executing {ch_shell_cmd}.")
    call(ch_shell_cmd)

    # insert temporary data to destination table
    ch_client.execute(
        f"INSERT INTO {dest_table} SELECT * FROM {tmp_table}"
    )


# Parse and process every message
def process_msg(message: pulsar.Message):
    # parse header of message
    msg_data = message.data()
    data_header, data = msg_data.split(b"\n", 1)
    data_header = json.loads(data_header.decode("utf8"))

    cur_id = data_header.get("sync_id")
    worker_name = data_header.get("worker_name")
    total_batch = data_header.get("total_batch")
    cur_batch = data_header.get("cur_batch")
    # partition = data_header.get("partition")
    tmp_table = data_header.get("tmp_table")
    dest_table = data_header.get("dest_table")
    # is_overwrite = data_header.get("is_overwrite", True)

    # reserve space for every body of message(bytes) batches, and store current batch
    if not local_cache.get(cur_id):
        local_cache[cur_id] = [b'' for _ in range(0, total_batch)]

    local_cache[cur_id][cur_batch - 1] = data

    data_header['recv_checksum'] = hashlib.md5(data).hexdigest()
    non_empty_length = len([1 for d in local_cache[cur_id] if d])

    # checksum verify
    if data_header['recv_checksum'] == data_header['md5_checksum']:
        print("Receiving :\n%s" % json.dumps(data_header, indent=2))
    else:
        print("Warning msg %s: %s checksum is different" %
              message.message_id(), json.dumps(data_header, indent=2))

    # if all batches of a file are received, compact the batches and free memory
    if non_empty_length >= total_batch:
        # compact file
        file_path = compact_file(worker_name, cur_id, local_cache[cur_id])
        # mark deletion
        del local_cache[cur_id]
        # flush data to ClickHouse
        flush_data_to_ch(file_path, tmp_table, dest_table)


if __name__ == '__main__':
    pulsar_url = "pulsar://pulsar-cluster01-slb:6650"
    topic = "persistent://bigdata/data_cross/mini_send_tb"
    sub_name = "cross_platform_xqc_data_sync"

    client = pulsar.Client(service_url=pulsar_url)
    consumer = client.subscribe(topic=topic, subscription_name=sub_name)

    while True:
        msg = consumer.receive()
        try:
            print("Received message id='%s'", msg.message_id())
            process_msg(msg)
            consumer.acknowledge(msg)
        except Exception as e:
            consumer.negative_acknowledge(msg)
            print(e)
            client.close()
