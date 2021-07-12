#!/usr/bin/python3
"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'TomAndersen',  # 设置拥有者
    'depends_on_past': False,  # 是否每个Task只有当前一个DAG的对应Task执行成功,才继续执行当前Task
    'start_date': datetime(2015, 6, 1),  # 设置start_date
    'email': ['airflow@example.com'],  # 设置邮件地址
    'email_on_failure': False,  # 设置失败后是否发送Email邮件
    'email_on_retry': False,
    'retries': 1,  # 设置失败重试次数
    'retry_delay': timedelta(minutes=5),  # 设置失败重试间隔
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='tutorial',  # 设置DAG_ID,必须唯一
    default_args=default_args,  # 设置DAG默认参数
    schedule_interval=timedelta(days=1),  # 设置DAG执行周期
    max_active_runs=1  # 设置同时运行的最大DAG个数
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
