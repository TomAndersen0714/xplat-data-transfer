from airflow import DAG
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.contrib.operators.impala_to_pulsar import ImpalaToPulsarOperator
from datetime import datetime, timedelta

imp_conn_id = 'cdh0_impala'
imp_source_table = 'tmp.range_partition_test'
kudu_dest_table = 'impala::tmp.range_partition_test'

pulsar_conn_id = 'cdh2_pulsar'
pulsar_topic = 'my-topic'

source_platform = 'local'
target_platform = 'local'

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 19),
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

imp_sql = """
    SELECT day,'Andersen' as str_type
    FROM tmp.range_partition_test
"""

header = PulsarHook.get_kudu_msg_header(
    source_table=imp_source_table,
    target_table=kudu_dest_table,
    source_platform=source_platform,
    target_platform=target_platform,
    # range_partition="{{ds_nodash}}<=VALUES<{{next_ds_nodash}}"
    range_partition="20210804<=VALUES<20210805"
)

# fetch data from clickhouse and send to pulsar
impala_to_kudu_test = ImpalaToPulsarOperator(
    task_id='impala_to_kudu_test',
    imp_conn_id=imp_conn_id,
    imp_sql=imp_sql,
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    header=header,
    dag=dag
)
