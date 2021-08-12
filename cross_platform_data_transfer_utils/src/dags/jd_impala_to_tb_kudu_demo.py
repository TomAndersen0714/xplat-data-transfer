from airflow import DAG
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.contrib.operators.impala_to_pulsar import ImpalaToPulsarOperator
from datetime import datetime, timedelta

# 基础配置
source_platform = 'jd'
target_platform = 'tb'
target_db = 'kudu'
job_id = 'demo_job_id'
interval = 'daily'
dag_id = f'{job_id}_{source_platform}_to_{target_platform}_{target_db}_{interval}'

# 数据源
imp_conn_id = 'cdh0_impala'
imp_source_table = 'tmp.chat_log_v1'

# 数据管道
pulsar_conn_id = 'pulsar_cluster01_slb'
pulsar_topic = f'persistent://bigdata/data_cross/{source_platform}_send_{target_platform}'

# 目标表
kudu_target_table = 'impala::tmp.test_kudu_target_tbl'

imp_sql = """
    SELECT `shop_id`, `snick`, `day`, `msg`, `platform`
    FROM tmp.chat_log_v1
"""

header = PulsarHook.get_kudu_msg_header(
    task_id="kudu_to_kudu_test",
    source_table=imp_source_table,
    target_table=kudu_target_table,
    source_platform=source_platform,
    target_platform=target_platform
)

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 4),
    'email': ['chengcheng@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=dag_id,
    description='impala_to_kudu_test',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)

# fetch data from clickhouse and send to pulsar
impala_to_kudu_test = ImpalaToPulsarOperator(
    task_id='impala_to_kudu_test',
    imp_conn_id=imp_conn_id,
    pulsar_conn_id=pulsar_conn_id,
    imp_sql=imp_sql,
    topic=pulsar_topic,
    header=header,
    dag=dag
)
