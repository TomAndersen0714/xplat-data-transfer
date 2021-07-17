from airflow import DAG
from airflow.contrib.operators.pulsar_to_clickhouse import PulsarToClickHouseOperator
from datetime import datetime, timedelta

ch_conn_id = 'cd2_clickhouse'
ch_dest_table = 'tmp.truncate_test_all'

pulsar_conn_id = 'cdh2_pulsar'
pulsar_topic = 'my-topic'
subscription = 'my-subscription'

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 10),
    'email': ['chengcheng@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='pulsar_to_ch_test',
    description='pulsar_to_ch_test',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)

# fetch data from pulsar to clickhouse
pulsar_to_ch_test = PulsarToClickHouseOperator(
    task_id='pulsar_to_ch_test',
    ch_conn_id=ch_conn_id,
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    subscription=subscription,
    dest_table=ch_dest_table,
    dag=dag
)
