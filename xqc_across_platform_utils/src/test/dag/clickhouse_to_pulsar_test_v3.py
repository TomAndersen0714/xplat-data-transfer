import uuid

from airflow import DAG
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from datetime import datetime, timedelta

ch_conn_id = 'cd2_clickhouse'
ch_source_table = 'tmp.truncate_test_all'
ch_dest_table_1 = 'tmp.truncate_test_all'
ch_clear_table_1 = 'tmp.truncate_test_local'
ch_dest_table_2 = 'tmp.truncate_test_all_1'
ch_clear_table_2 = 'tmp.truncate_test_local_1'

pulsar_conn_id = 'cdh2_pulsar'
pulsar_topic = 'my-topic'

batch_id = str(uuid.uuid4())

header1 = {
    "task_id": "ch_to_pulsar_test",
    "db_type": "clickhouse",
    "target_table": ch_dest_table_1,
    "clear_table": ch_clear_table_1,
    "partition": "{{ds_nodash}}",
    "cluster_name": "cluster_3s_2r",
    "batch_id": batch_id
}

header2 = {
    "task_id": "ch_to_pulsar_test",
    "db_type": "clickhouse",
    "target_table": ch_dest_table_2,
    "clear_table": ch_clear_table_2,
    "partition": "{{ds_nodash}}",
    "cluster_name": "cluster_3s_2r",
    "batch_id": batch_id
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
    dag_id='ch_to_pulsar_test_v2',
    description='ch_to_pulsar_test',
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
