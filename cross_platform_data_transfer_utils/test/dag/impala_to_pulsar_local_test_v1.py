from airflow import DAG
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.contrib.operators.impala_to_pulsar import ImpalaToPulsarOperator
from datetime import datetime, timedelta

imp_conn_id = 'cdh0_impala'
imp_source_table = 'tmp.chat_log_v1'
kudu_dest_table = 'impala::tmp.test_kudu_target_tbl'
imp_sql = """
    SELECT `shop_id`, `snick`, `day`, `msg`, `platform`
    FROM tmp.chat_log_v1
"""

pulsar_conn_id = 'cdh2_pulsar'
pulsar_topic = 'my-topic'

source_platform = 'local'
target_platform = 'local'

header = PulsarHook.get_kudu_msg_header(
    task_id="kudu_to_kudu_test",
    source_table=imp_source_table,
    target_table=kudu_dest_table,
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
    dag_id='impala_to_kudu_test',
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
