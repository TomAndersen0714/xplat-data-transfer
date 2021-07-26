import logging
import time

from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'yangboxiao',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 23),
    'email': ['yangboxiao@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'jd_daily_xdata_sync_to_tb',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=3
)

header = {
    "task_id": dag.dag_id,
    "db_type": "clickhouse",
    "target_table": "buffer.pub_app_shop_overview_buffer",
    "partition": '{{ ds_nodash }}'
}

CH_HOOK_ID = 'clickhouse_jd'
SOURCE_PLAT = 'jd'
DEST_PLAT = 'tb'
PULSAR_CONN_ID = 'pulsar_cluster01_slb'
PULSAR_TOPIC = f'persistent://bigdata/data_cross/{SOURCE_PLAT}_send_{DEST_PLAT}'


def pub_app_shop_overview_all_func(ds_nodash):
    day_int = int(ds_nodash)

    ch = ClickHouseHook(CH_HOOK_ID)
    # 清空当天的数据
    clean_sql = f"""alter table pub_app.shop_overview_all drop partition {day_int}"""
    logging.info(clean_sql)
    ch.execute(clean_sql)
    time.sleep(1)
    ch_sql = f"""
insert into pub_app.shop_overview_all
select 'jd' as platform,
shop_recv.shop_id,
if(served_pv is null,0,served_pv) as served_pv,
if(served_uv is null,0,served_uv) as served_uv,
if(received_pv is null,0,received_pv) as received_pv,
if(received_uv is null,0,received_uv) as received_uv,
if(created_uv is null,0,created_uv) as created_uv,
if(created_order_cnt is null,0,created_order_cnt) as created_order_cnt,
if(created_payment is null,0,created_payment)/100 as created_payment,
if(paid_uv is null,0,paid_uv) as paid_uv,
if(paid_order_cnt is null,0,paid_order_cnt) as paid_order_cnt,
if(paid_payment is null,0,paid_payment)/100 as paid_payment,
0 as refund_uv,
0 as refund_order_cnt,
0 as refund_order_num,
0 as refund_payment,
{day_int} as day
from 
( select shop._id as shop_id,served_uv,served_pv,received_uv,received_pv
from 
dim.shop_nick_all as shop left join 
(
select 
shop_id,
count(distinct if(act = 'send_msg',cnick,NULL)) as served_uv,
sum(if(act = 'send_msg',1,0))  as served_pv, 
count(distinct if(act = 'recv_msg',cnick,NULL))  as received_uv, 
sum(if(act = 'recv_msg',1,0)) as received_pv 
from   ods.xdrs_logs_all where day = {day_int}
group by
shop_id 
) as shop_info 
 on shop._id = shop_info.shop_id
) as shop_recv 
left join
(
select 
shop_id,
count(distinct if(status = 'created',buyer_nick,null)) as created_uv,
count(distinct if(status = 'created',order_id,null)) as created_order_cnt,
sum(if(status = 'created',payment,0) ) as created_payment,
count(distinct if(status = 'paid',buyer_nick,null)) as paid_uv,
count(distinct if(status = 'paid',order_id,null)) as paid_order_cnt,
sum(if(status = 'paid',payment,0)) as paid_payment
from  ods.order_event_all where day = {day_int} 
group by 
shop_id
) as shop_order 
using(shop_id)
    """
    logging.info(ch_sql)
    rv = ch.execute(ch_sql)
    logging.info(rv)


pub_app_shop_overview_all = PythonOperator(
    task_id="pub_app_shop_overview_all",
    python_callable=pub_app_shop_overview_all_func,
    op_kwargs={
        "ds_nodash": "{{ ds_nodash }}"
    },
    dag=dag
)

pub_app_shop_overview_all_to_pulsar = ClickHouseToPulsarOperator(
    task_id='pub_app_shop_overview_all_to_pulsar',
    ch_conn_id=CH_HOOK_ID,
    ch_query_sql='SELECT * FROM pub_app.shop_overview_all where day = {{ ds_nodash }}',
    pulsar_conn_id=PULSAR_CONN_ID,
    topic=PULSAR_TOPIC,
    header=header,
    dag=dag
)

pub_app_shop_overview_all >> pub_app_shop_overview_all_to_pulsar
