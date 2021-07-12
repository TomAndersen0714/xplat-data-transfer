#!/usr/bin/python3
import logging
from time import sleep

from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.contrib.operators.mongo_to_clickhouse_operator import MongoToClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# configuration
work_id = 'xdqc_kefu_stat_sync_produce_daily'

# data source
mongo_conn_id = 'xdqc_mongo'
mongo_db = 'xdqc-tb'
mongo_collection = 'kefu_stat'

# data destination
ch_conn_id = 'clickhouse_v1mini-bigdata-002'
ch_conn_tcp_port = 19000
ch_container_id = 'd57091c972e7'
ch_container_tmp_dir = '/var/lib/clickhouse/data/tmp'
ch_container_export_base_dir = f'{ch_container_tmp_dir}/{work_id}'

ch_tmp_local_table = 'tmp.xdqc_kefu_stat_daily_local'
ch_tmp_dist_table = 'tmp.xdqc_kefu_stat_daily_all'
ch_dest_table = 'xqc_ods.xdqc_kefu_stat_all'

pulsar_conn_id = 'pulsar_cluster01_slb'
pulsar_topic = 'persistent://bigdata/xqc_cross_platform/ch_data_sync'

ch_data_export_cmd = f"""
docker exec {ch_container_id} bash -c 
"clickhouse-client --port={ch_conn_tcp_port} --query  
'SELECT * FROM {ch_tmp_dist_table} FORMAT Parquet ' 
> {ch_container_export_base_dir}/{work_id}"+'_{{ ds_nodash }}.parquet' 
"""

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 7),
    'email': ['chengcheng@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=work_id,
    description='MongoDB:xdqc-tb.kefu_stat增量同步小程序,快手,老淘宝,融合版CH集群',
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


# 删除本地 ClickHouse 临时表
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

# MongoDB 增量数据导入本地 ClickHouse 服务器临时表
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


# 将 ClickHouse 临时表数据写入汇总表
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


# 检查导出路径,执行导出命令
def check_and_export(path, cmd):
    logging.info(f'Exporting data to {path}')
    logging.info(f'Executing :{cmd}')

    import os
    if not os.path.exists(path):
        os.mkdir(path)
    os.system(cmd)


# 本地 ClickHouse 服务器导出数据到本地 Parquet
kefu_stat_ch_to_local = PythonOperator(
    task_id='kefu_stat_ch_to_local',
    python_callable=check_and_export,
    op_kwargs={
        'path': ch_container_export_base_dir,
        'cmd': ch_data_export_cmd
    },
    dag=dag
)


# 将本地 Parquet 文件发送给指定 Pulsar
def local_to_pulsar(file_path):
    pulsar_hook = PulsarHook(pulsar_conn_id=pulsar_conn_id)
    pulsar_hook.send_files([file_path], pulsar_topic)


kefu_stat_local_to_pulsar = PythonOperator(
    task_id='kefu_stat_local_to_pulsar',
    python_callable=local_to_pulsar,
    op_kwargs={
        'file_path': f'{ch_container_export_base_dir}/{work_id}' + '_{{ ds_nodash }}.parquet'
    },
    dag=dag
)

kefu_stat_truncate_ch_tmp_table >> kefu_stat_mongo_to_ch_tmp >> kefu_stat_ch_tmp_to_all >> kefu_stat_ch_to_local >> kefu_stat_local_to_pulsar
