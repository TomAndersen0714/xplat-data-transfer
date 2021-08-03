import uuid

from airflow import DAG
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from datetime import datetime, timedelta

# 基础配置
source_platform = 'jd'
dest_platform = 'tb'
db = 'ch'
job_id = 'demo_id'  # 自定义
interval = 'daily'  # 自定义
dag_id = f'{job_id}_{source_platform}_to_{dest_platform}_{db}_{interval}'
batch_id = str(uuid.uuid4())

# 数据源
ch_conn_id = f'clickhouse_{source_platform}'
ch_tmp_dist_table = 'tmp.xdqc_kefu_stat_daily_all'

# 数据终点
pulsar_conn_id = 'pulsar_cluster01_slb'
pulsar_topic = f'persistent://bigdata/data_cross/{source_platform}_send_{dest_platform}'

# 源表和目标表
ch_source_table = 'tmp.truncate_test_all'
ch_dest_table_1 = 'tmp.truncate_test_all'
ch_clear_table_1 = 'tmp.truncate_test_local'
ch_dest_table_2 = 'tmp.truncate_test_all_1'
ch_clear_table_2 = 'tmp.truncate_test_local_1'
ch_dest_table_3 = 'tmp.drop_partition_test_all'
ch_clear_table_3 = 'tmp.drop_partition_test_local'

header1: dict[str:str] = {
    "task_id": "ch_to_pulsar_test",  # 自定义
    "db_type": "clickhouse",  # 自定义
    "source_platform": source_platform,
    "dest_platform": dest_platform,
    "source_table": ch_source_table,  # 数据源表
    "target_table": ch_dest_table_1,  # 需要写入的 buffer 或 distributed 表
    "clear_table": ch_clear_table_1,  # 需要清除的local表
    "partition": "(2021,7,13)",  # 数值字段组成的分区键表达式, 直接使用元组字符串形式
    "cluster_name": "cluster_3s_2r",  # 集群配置有分区名则写分区名, 未配置则不写
    "batch_id": batch_id  # 用于唯一标识当前批次的消息, 每个批次必须不同
}

header2: dict[str:str] = {
    "task_id": "ch_to_pulsar_test",
    "db_type": "clickhouse",
    "source_platform": source_platform,
    "dest_platform": dest_platform,
    "source_table": ch_source_table,
    "target_table": ch_dest_table_2,
    "clear_table": ch_clear_table_2,
    "partition": "(2021,'Tom','Andersen')",  # 多个分区键组成的分区表达式, 使用元组字符串形式, 其中的字符串通用用单引号标识
    "cluster_name": "cluster_3s_2r",
    "batch_id": batch_id
}

header3: dict[str:str] = {
    'task_id': 'test',
    "db_type": "clickhouse",
    "source_platform": source_platform,
    "dest_platform": dest_platform,
    "source_table": ch_source_table,
    "target_table": ch_dest_table_3,
    "clear_table": ch_clear_table_3,
    "partition": "'jd'",  # 单个分区键,如果是字符串类型需要加上单引号与数值类型进行区分
    "cluster_name": "cluster_3s_2r",
    "batch_id": batch_id
}

default_args = {
    'owner': 'xiaoduo',  # 自定义
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 14),  # 自定义
    'email': ['demo@xiaoduotech.com'],  # 自定义
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=dag_id,
    description='ch_to_pulsar_test',  # 自定义
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)

# fetch data from clickhouse and send to pular
ch_to_pulsar_test_1 = ClickHouseToPulsarOperator(
    task_id='ch_to_pulsar_test_1',
    ch_conn_id=ch_conn_id,
    ch_query_sql=f'SELECT * FROM {ch_source_table}',
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    header=header1,
    dag=dag
)

# fetch data from clickhouse and send to pular
ch_to_pulsar_test_2 = ClickHouseToPulsarOperator(
    task_id='ch_to_pulsar_test_2',
    ch_conn_id=ch_conn_id,
    ch_query_sql=f'SELECT * FROM {ch_source_table}',
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    header=header2,
    dag=dag
)

# fetch data from clickhouse and send to pular
ch_to_pulsar_test_3 = ClickHouseToPulsarOperator(
    task_id='ch_to_pulsar_test_3',
    ch_conn_id=ch_conn_id,
    ch_query_sql=f'SELECT * FROM {ch_source_table}',
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    header=header3,
    dag=dag
)
