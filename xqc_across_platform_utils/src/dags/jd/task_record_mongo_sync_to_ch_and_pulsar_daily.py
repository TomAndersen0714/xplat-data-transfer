#!/usr/bin/python3
import logging

from time import sleep
from typing import Optional, Dict
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from airflow.contrib.operators.mongo_to_clickhouse_operator import MongoToClickHouseOperator
from airflow.operators.python_operator import PythonOperator

# data source
mongo_conn_id = 'xdqc_mongo'
mongo_db = 'xdqc-tb'
mongo_collection = 'task_record'
source_platform = 'jd'
dest_platform = 'tb'

# configuration
dag_id = f'xqc_xdqc_tb_task_record_{source_platform}_to_{dest_platform}_ch_daily'

# data transfer station
ch_conn_id = f'clickhouse_{source_platform}'
ch_tmp_local_table = 'tmp.xdqc_tb_task_record_local'
ch_tmp_dist_table = ch_tmp_local_table
ch_dest_local_table = 'xqc_ods.xdqc_tb_task_record_local'
ch_dest_dist_table = ch_dest_local_table

# data destination
pulsar_conn_id = 'pulsar_cluster01_slb'
pulsar_topic = f'persistent://bigdata/data_cross/{source_platform}_send_{dest_platform}'
db_type = 'clickhouse'
ch_dest_buffer_table = 'buffer.xdqc_tb_task_record_buffer'
partition = " '%s',intDiv({{ ds_nodash }},100) " % 'jd'

header: Optional[Dict[str, str]] = {
    "task_id": dag_id,
    "db_type": db_type,
    "target_table": ch_dest_buffer_table,
    "partition": partition
}

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 20),
    'email': ['chengcheng@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=dag_id,
    description='MongoDB:xdqc-tb.task_record增量跨平台同步CH集群',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)


# MongoDB 增量查询过滤条件
def daily_delta_query(ds_nodash):
    return [
        {"$match":
             {"date": int(ds_nodash)}
         }
    ]


# 清空 ClickHouse 临时表
def truncate_ch_table(table_name):
    ch_hook = ClickHouseHook(ch_conn_id)
    ch_hook.execute(f"truncate table {table_name}")
    logging.info(f"truncate table {table_name}")
    sleep(3)


task_record_truncate_ch_tmp_table = PythonOperator(
    task_id='task_record_truncate_ch_tmp_table',
    python_callable=truncate_ch_table,
    op_kwargs={
        'table_name': ch_tmp_local_table
    },
    dag=dag
)

# MongoDB 增量数据导入本地 ClickHouse 服务器临时的分布式表
task_record_mongo_to_ch_tmp = MongoToClickHouseOperator(
    task_id='task_record_mongo_to_ch_tmp',
    mongo_conn_id=mongo_conn_id,
    mongo_db=mongo_db,
    mongo_collection=mongo_collection,
    clickhouse_conn_id=ch_conn_id,
    destination_ch_table=ch_tmp_dist_table,
    aggregate_query=None,
    aggregate_func=daily_delta_query,
    aggregate_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    dag=dag
)


# 删除 ClickHouse 汇总表中对应的分区(单节点不能加ON CLUSTER子句)
def ch_drop_partition(table, partition):
    ch_hook = ClickHouseHook(ch_conn_id)
    partition = str(ch_hook.execute(f"SELECT {partition}")[0])
    ch_hook.execute(f"alter table {table} drop partition {partition}")
    logging.info(f"alter table {table} drop partition {partition}")
    sleep(3)


task_record_ch_drop_partition = PythonOperator(
    task_id='task_record_ch_drop_partition',
    python_callable=ch_drop_partition,
    op_kwargs={
        'table': ch_dest_local_table,
        'partition': partition
    },
    dag=dag
)


# 将 ClickHouse 临时表数据刷入汇总表
def transport_ch_data(source_table, dest_table):
    ch_hook = ClickHouseHook(ch_conn_id)
    transport_sql = f'INSERT INTO {ch_dest_dist_table} SELECT * FROM {ch_tmp_dist_table}'
    logging.info(f'Export data from {source_table} to {dest_table}')
    logging.info(transport_sql)
    ch_hook.execute(transport_sql)


task_record_ch_tmp_to_all = PythonOperator(
    task_id='task_record_ch_tmp_to_all',
    python_callable=transport_ch_data,
    op_kwargs={
        'source_table': ch_tmp_dist_table,
        'dest_table': ch_dest_dist_table
    },
    dag=dag
)

# 查询 ClickHouse 临时分布式表内容,并将其发送到跨平台 Pulsar 集群
task_record_ch_tmp_to_pulsar = ClickHouseToPulsarOperator(
    task_id='task_record_ch_tmp_to_pulsar',
    ch_conn_id=ch_conn_id,
    ch_query_sql=f'SELECT * FROM {ch_tmp_dist_table}',
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    header=header,
    dag=dag
)

task_record_truncate_ch_tmp_table >> task_record_mongo_to_ch_tmp >> \
task_record_ch_drop_partition >> task_record_ch_tmp_to_all >> task_record_ch_tmp_to_pulsar
