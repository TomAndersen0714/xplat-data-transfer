#!/usr/bin/python3

from airflow import DAG
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from datetime import datetime, timedelta
from typing import Optional, Dict

# configuration
dest_platform = 'jd'
source_platform = 'tb'
db = 'ch'
job_id = 'xqc_xdqc_kefu_stat'
dag_id = f'{job_id}_{source_platform}_to_{dest_platform}_{db}_daily'

# data source
ch_conn_id = f'clickhouse_{source_platform}'
ch_source_table = 'tmp.xdqc_kefu_stat_daily_all'

# data destination
pulsar_conn_id = 'pulsar_cluster01_slb'
pulsar_topic = f'persistent://bigdata/data_cross/{source_platform}_send_{dest_platform}'
ch_dest_table = "buffer.xdqc_kefu_stat_buffer"
partition = "{{ds_nodash}}"
db_type = "clickhouse"

header: Optional[Dict[str, str]] = {
    "task_id": dag_id,
    "db_type": db_type,
    "target_table": ch_dest_table,
    "partition": partition
}

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 15),
    'email': ['demo@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=dag_id,
    description='增量跨平台同步CH集群',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)

# 查询 ClickHouse 临时分布式表内容,并将其发送到跨平台 Pulsar 集群
kefu_stat_ch_tmp_to_pulsar = ClickHouseToPulsarOperator(
    task_id='kefu_stat_ch_tmp_to_pulsar',
    ch_conn_id=ch_conn_id,
    ch_query_sql=f'SELECT * FROM {ch_source_table}',
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    header=header,
    dag=dag
)
