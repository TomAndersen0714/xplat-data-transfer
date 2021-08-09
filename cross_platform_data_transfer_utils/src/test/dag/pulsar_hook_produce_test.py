import logging
from time import sleep

from airflow import DAG
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

pulsar_conn_id = 'cdh2_pulsar'
topic = 'my-topic'

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 8),
    'email': ['chengcheng@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='pulsar_hook_produce_test',
    description='pulsar_hook_produce_test',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)


# produce message
def msg_producer():
    pulsar_hook = PulsarHook(pulsar_conn_id=pulsar_conn_id, topic=topic)

    for i in range(10):
        logging.info('Sending test message %s' % i)
        pulsar_hook.send_msg(('Test message %s' % i).encode('utf8'))
        sleep(3)

    pulsar_hook.close()


pular_hook_test_producer = PythonOperator(
    task_id='pular_hook_test_producer',
    python_callable=msg_producer,
    dag=dag
)
