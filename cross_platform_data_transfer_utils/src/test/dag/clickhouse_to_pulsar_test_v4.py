from airflow import DAG
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from datetime import datetime, timedelta

ch_conn_id = 'cdh2_clickhouse'
ch_source_table = 'tmp.truncate_test_all'
ch_dest_table = 'tmp.truncate_test_all'
source_platform = 'jd'
dest_platform = 'tb'

pulsar_conn_id = 'cdh2_pulsar'
pulsar_topic = 'my-topic'

header = PulsarHook.get_ch_msg_header(
    task_id="ch_to_pulsar_test",
    source_table=ch_source_table,
    target_table=ch_dest_table
)

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 8),
    'email': ['chengcheng@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='ch_to_pulsar_test_v1',
    description='ch_to_pulsar_test',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)

# fetch data from clickhouse and send to pulsar
ch_to_pulsar_test = ClickHouseToPulsarOperator(
    task_id='ch_to_pulsar_test',
    ch_conn_id=ch_conn_id,
    ch_query_sql=f'SELECT `b`,`a` FROM {ch_source_table}',
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    header=header,
    dag=dag
)
