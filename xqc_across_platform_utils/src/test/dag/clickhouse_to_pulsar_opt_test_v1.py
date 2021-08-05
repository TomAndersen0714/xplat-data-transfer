from airflow import DAG
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from datetime import datetime, timedelta

ch_conn_id = 'cdh2_clickhouse'
ch_source_table = 'tmp.truncate_test_all'
ch_dest_table = 'tmp.truncate_test_all'

pulsar_conn_id = 'cdh2_pulsar'
pulsar_topic = 'my-topic'

header = {
    "task_id": "ch_to_pulsar_test",
    "db_type": "clickhouse",
    "target_table": ch_dest_table,
    "partition": "{{ds_nodash}}"
}

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 14),
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

# fetch data from clickhouse and send to pular
ch_to_pulsar_test = ClickHouseToPulsarOperator(
    task_id='ch_to_pulsar_test',
    ch_conn_id=ch_conn_id,
    ch_query_sql=f'SELECT * FROM {ch_source_table}',
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    header=header,
    dag=dag
)
