#!/usr/bin/python3
import logging

from time import sleep
from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.operators.pulsar_to_clickhouse import PulsarToClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# configuration
job_id = 'xqc_xdqc_kefu_stat_sync'
dag_id = f'{job_id}_to_ch_daily'

# data source
platform = 'taobao'
pulsar_conn_id = 'pulsar_cluster01_slb'
pulsar_topic = f'persistent://bigdata/cross_platform/{job_id}'
subscription = f'{job_id}_{platform}'

# data destination
ch_conn_id = 'clickhouse_zjk_008'
ch_tmp_local_table = 'tmp.xdqc_kefu_stat_daily_local'
ch_tmp_dist_table = 'tmp.xdqc_kefu_stat_daily_all'
ch_dest_table = 'xqc_ods.xdqc_kefu_stat_all'

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
    description='CH每日增量同步 xqc_ods.xdqc_kefu_stat_all',
    default_args=default_args,
    schedule_interval="31 5 * * *",
    max_active_runs=1,
    concurrency=2
)


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

# 接收 Pulsar 消息将数据插入到 ClickHouse 临时表
xdqc_kefu_pulsar_to_ch_tmp = PulsarToClickHouseOperator(
    task_id='xdqc_kefu_pulsar_to_ch_tmp',
    ch_conn_id=ch_conn_id,
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    subscription=subscription,
    dest_table=ch_tmp_dist_table,
    dag=dag
)


# 将 ClickHouse 临时表数据刷入汇总表
def transport_ch_data(source_table, dest_table):
    logging.info(f'Export data from {source_table} to {dest_table}')

    ch_hook = ClickHouseHook(ch_conn_id)
    transport_sql = f'INSERT INTO {ch_dest_table} SELECT * FROM {ch_tmp_dist_table}'
    ch_hook.execute(transport_sql)


kefu_stat_ch_tmp_to_all = PythonOperator(
    task_id='kefu_stat_ch_tmp_to_all',
    python_callable=transport_ch_data,
    op_kwargs={
        'source_table': ch_tmp_dist_table,
        'dest_table': ch_dest_table
    },
    dag=dag
)

kefu_stat_truncate_ch_tmp_table >> xdqc_kefu_pulsar_to_ch_tmp >> kefu_stat_ch_tmp_to_all
