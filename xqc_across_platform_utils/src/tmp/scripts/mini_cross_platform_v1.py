import pulsar
import json
from subprocess import call
from retrying import retry
import os
import shutil
import logging
from logging import handlers
import hashlib

# Log handler
log_handler = handlers.RotatingFileHandler("./data_sync.log", maxBytes=1024 * 1024 * 100)
log_handler.setLevel(logging.DEBUG)
logger = logging.getLogger("root")
logger.addHandler(log_handler)

# Impala SQL
hdfs_tmp_dir = "/user/xd_stat/data_sync"
add_partition_sql = "alter table {table} add if not exists {partition_clause}"
load_data_overwrite_sql = "load data inpath '{data_dir}' overwrite into table {table} {partition_clause};"
load_data_into_sql = "load data inpath '{data_dir}' into table {table} {partition_clause};"
IMPALA_RESERVE_WORDS = ('date', 'data', 'no',)

# Cache for all received parts
local_cache = {}

# Add single quote to the beginning and end of string
def str_fmt(s):
    return "'%s'" % s

# Generate partition condition string and add '`' symbol for reserved words of impala
# (e.g. "`date`=20190918","`date`='20190918'")
def get_par_cond_paid(partition):
    return ','.join([f"{'`%s`' % k if k in IMPALA_RESERVE_WORDS else k}={str_fmt(v) if isinstance(v, str) else v}"
                     for k, v in partition.items()])

# Execute specific commands, and retry 3 times until succeed
@retry(
    stop_max_attempt_number=3,
    wait_fixed=2000,
    retry_on_result=lambda x: x != 0)
def _call(cmd_list):
    print(f"Exec {cmd_list}")
    return call(cmd_list)


def flush_data_to_impala(cur_id, table, partition, data_list, is_overwrite=True):
    print(f"Try load data {cur_id} to impala ...")
    data_dir = f"{hdfs_tmp_dir}/{cur_id}"
    partition_clause = f"partition({get_par_cond_paid(partition)})" if partition else ""
    local_tmp_dir = f"/tmp/data_sync/{cur_id}"
    try:
        shutil.rmtree(local_tmp_dir)
    except:
        pass
    os.makedirs(local_tmp_dir, exist_ok=True)
    for d in data_list:
        with open(f"/tmp/data_sync/{cur_id}/0.parq", 'ab') as f:
            f.write(d)
    try:
        _call(["hadoop", "fs", "-rm", "-r", data_dir])
    except:
        pass
    _call(["hadoop", "fs", "-put", local_tmp_dir, data_dir])
    _call(["hadoop", "fs", "-chmod", "777", data_dir])
    _call(["impala-shell", "-q", add_partition_sql.format(table=table, partition_clause=partition_clause)])
    if is_overwrite:
        _call(["impala-shell", "-q",
               load_data_overwrite_sql.format(data_dir=data_dir, table=table, partition_clause=partition_clause)])
    else:
        _call(["impala-shell", "-q",
               load_data_into_sql.format(data_dir=data_dir, table=table, partition_clause=partition_clause)])
    _call(["hadoop", "fs", "-rm", "-r", data_dir])
    shutil.rmtree(local_tmp_dir)
    print(f"Load data {cur_id} to impala succeeded!")

# Compact all parts of same file into one
def done_to_file(worekr_name, cur_id, table, partition, data_list, is_overwrite=True):
    print(f"Try load data {worekr_name} {cur_id} to impala ...")
    data_dir = f'/data4/code_workspace/recv_data/{worekr_name}'
    try:
        shutil.rmtree(data_dir)
    except:
        pass
    os.makedirs(data_dir, exist_ok=True)
    for d in data_list:
        with open(f"{data_dir}/{worekr_name}_{cur_id}.parq", 'ab') as f:
            f.write(d)


def process_msg(msg_data):
    data_header, data = msg_data.split(b"\n", 1)
    data_header = json.loads(data_header.decode("utf8"))
    cur_id = data_header.get("sync_id")
    worekr_name = data_header.get("worekr_name")
    total_parts = data_header.get("total_parts")
    partition = data_header.get("partition")
    table = data_header.get("table")
    cur_part = data_header.get("cur_part")
    is_overwrite = data_header.get("is_overwrite", True)
    if not local_cache.get(cur_id):
        local_cache[cur_id] = [b'' for i in range(0, total_parts)]
    local_cache[cur_id][cur_part - 1] = data
    data_header['recv_checksum'] = hashlib.md5(data).hexdigest()

    non_empty_length = len([1 for d in local_cache[cur_id] if d])
    print("Recv :\n%s" % json.dumps(data_header, indent=2))
    if non_empty_length >= total_parts:
        done_to_file(worekr_name, cur_id, table, partition, local_cache[cur_id], is_overwrite)
        del local_cache[cur_id]


if __name__ == '__main__':
    pulsar_url = "pulsar://pulsar-cluster01-slb:6650"
    topic = "persistent://bigdata/data_cross/mini_send_tb"
    sub = "bigdata_data_sync"
    client = pulsar.Client(pulsar_url)
    consumer = client.subscribe(topic, sub)

    while True:
        try:
            msg = consumer.receive()
            process_msg(msg.data())
        except Exception as e:
            print(e)
        finally:
            consumer.acknowledge(msg)

