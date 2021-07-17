#!/usr/bin/python3
import logging

from time import sleep
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from airflow.contrib.operators.mongo_to_clickhouse_operator import MongoToClickHouseOperator
from airflow.operators.python_operator import PythonOperator

# configuration
job_id = 'xqc_xdqc_kefu_stat_sync'
dag_id = f'{job_id}_to_ch_and_pulsar_daily'

# data source
mongo_conn_id = 'xdqc_mongo'
mongo_db = 'xdqc-tb'
mongo_collection = 'kefu_stat'

# data transfer station
ch_conn_id = 'clickhouse_v1mini-bigdata-002'
ch_tmp_local_table = 'tmp.xdqc_kefu_stat_daily_local'
ch_tmp_dist_table = 'tmp.xdqc_kefu_stat_daily_all'
ch_dest_local_table = 'xqc_ods.xdqc_kefu_stat_local'
ch_dest_dist_table = 'xqc_ods.xdqc_kefu_stat_all'

# data destination
pulsar_conn_id = 'pulsar_cluster01_slb'
pulsar_topic = 'persistent://bigdata/data_cross/mini_send_tb'

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 12),
    'email': ['chengcheng@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=dag_id,
    description='MongoDB:xdqc-tb.kefu_stat增量跨平台同步CH集群',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)


# MongoDB 增量查询过滤条件
def daily_delta_query(ds_nodash):
    return [
        {"$match": {
            "date": {
                "$gte": int(ds_nodash)}
        }
        }
    ]


# 清空 ClickHouse 临时表
def truncate_ch_table(table_name):
    ch_hook = ClickHouseHook(ch_conn_id)
    ch_hook.truncate_table(table_name)
    sleep(3)


kefu_stat_truncate_ch_tmp_table = PythonOperator(
    task_id='kefu_stat_truncate_ch_tmp_table',
    python_callable=truncate_ch_table,
    op_kwargs={
        'table_name': ch_tmp_local_table
    },
    dag=dag
)

# MongoDB 增量数据导入本地 ClickHouse 服务器临时的分布式表
kefu_stat_mongo_to_ch_tmp = MongoToClickHouseOperator(
    task_id='kefu_stat_mongo_to_ch_tmp',
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


# 删除 ClickHouse 汇总表中对应的分区
def ch_drop_partition(table, partition):
    ClickHouseHook(ch_conn_id).drop_partition(table, partition)
    sleep(3)


kefu_stat_ch_drop_partition = PythonOperator(
    task_id='kefu_stat_ch_drop_partition',
    python_callable=ch_drop_partition,
    op_kwargs={
        'table': ch_dest_local_table,
        'partition': "{{ ds_nodash }}"
    },
    dag=dag
)


# 将 ClickHouse 临时表数据刷入汇总表
def transport_ch_data(source_table, dest_table):
    logging.info(f'Export data from {source_table} to {dest_table}')

    ch_hook = ClickHouseHook(ch_conn_id)
    transport_sql = f'INSERT INTO {ch_dest_dist_table} SELECT * FROM {ch_tmp_dist_table}'
    ch_hook.execute(transport_sql)


kefu_stat_ch_tmp_to_all = PythonOperator(
    task_id='kefu_stat_ch_tmp_to_all',
    python_callable=transport_ch_data,
    op_kwargs={
        'source_table': ch_tmp_dist_table,
        'dest_table': ch_dest_dist_table
    },
    dag=dag
)

kefu_stat_truncate_ch_tmp_table >> kefu_stat_mongo_to_ch_tmp >> \
kefu_stat_ch_drop_partition >> kefu_stat_ch_tmp_to_all
