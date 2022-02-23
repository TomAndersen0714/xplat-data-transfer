import logging
from time import sleep

from airflow import DAG
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

pulsar_conn_id = 'cdh2_pulsar'
topic = 'my-topic'
subscription = 'my-subscription'

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
    dag_id='pulsar_hook_consume_test',
    description='pulsar_hook_consume_test',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)


# consume message
def msg_consumer():
    pulsar_hook = PulsarHook(pulsar_conn_id=pulsar_conn_id, topic=topic)

    gen = pulsar_hook.consume_msg_generator(sub_name=subscription)

    for msg in gen:
        data = msg.data()
        content = data.decode('utf8')
        logging.info("Received message '%s' id='%s'", content, msg.message_id())

        if data == PulsarHook.__END_SIGN__:
            logging.info('Receive END_SIGN and stop current stream!')
            break

        sleep(3)


pular_hook_test_consumer = PythonOperator(
    task_id='pular_hook_test_consumer',
    python_callable=msg_consumer,
    dag=dag
)
