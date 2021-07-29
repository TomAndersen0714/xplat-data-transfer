from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from airflow.contrib.operators.mongo_to_clickhouse_operator import MongoToClickHouseOperator
from airflow.operators.python_operator import PythonOperator

DAG_ID = 'xinghuan_mc_sync_to_ch_and_tb'

MONGO_CONN_ID = 'xdqc_mongo'
CH_CONN_ID = 'clickhouse'
PULSAR_CONN_ID = 'pulsar-cluster01-slb'
PULSAR_TOPIC = 'persistent://bigdata/data_cross/mini_send_tb'

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 15),
    'email': ['demo@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=DAG_ID,
    description='每日同步明察数据并进行跨平台传输',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=1
)


def truncate_tmp_tbl():
    TMP_TABLES = [
        'tmp.xdqc_tb_shop', 'tmp.xinghuan_mc_company_shop',
        'tmp.xinghuan_mc_company', 'tmp.xinghuan_mc_xdmp_shop'
    ]
    truncate_ch_sql = "truncate table {table_name}"
    ch_hook = ClickHouseHook(CH_CONN_ID)
    for tmp_table in TMP_TABLES:
        ch_hook.execute(truncate_ch_sql.format(table_name=tmp_table))


truncate_ch_tmp_table = PythonOperator(
    task_id='truncate_ch_tmp_table',
    python_callable=truncate_tmp_tbl,
    dag=dag
)

mongo_xdqc_tb_shop_to_ch_tmp = MongoToClickHouseOperator(
    task_id='mongo_xdqc_tb_shop_to_ch_tmp',
    mongo_conn_id=MONGO_CONN_ID,
    mongo_db='xdqc-tb',
    mongo_collection='shop',
    clickhouse_conn_id=CH_CONN_ID,
    destination_ch_table='tmp.xdqc_tb_shop',
    aggregate_query=None,
    dag=dag
)

mongo_xinghuan_mc_company_shop_to_ch_tmp = MongoToClickHouseOperator(
    task_id='mongo_xinghuan_mc_company_shop_to_ch_tmp',
    mongo_conn_id=MONGO_CONN_ID,
    mongo_db='xinghuan-mc',
    mongo_collection='company_shop',
    clickhouse_conn_id=CH_CONN_ID,
    destination_ch_table='tmp.xinghuan_mc_company_shop',
    aggregate_query=None,
    dag=dag
)

mongo_xinghuan_mc_company_to_ch_tmp = MongoToClickHouseOperator(
    task_id='mongo_xinghuan_mc_company_to_ch_tmp',
    mongo_conn_id=MONGO_CONN_ID,
    mongo_db='xinghuan-mc',
    mongo_collection='company_shop',
    clickhouse_conn_id=CH_CONN_ID,
    destination_ch_table='tmp.xinghuan_mc_company',
    aggregate_query=None,
    dag=dag
)

mongo_xinghuan_mc_tmp_xdmp_shop_to_ch_tmp = MongoToClickHouseOperator(
    task_id='mongo_xinghuan_mc_tmp_xdmp_shop_to_ch_tmp',
    mongo_conn_id=MONGO_CONN_ID,
    mongo_db='xinghuan-mc',
    mongo_collection='tmp_xdmp_shop',
    clickhouse_conn_id=CH_CONN_ID,
    destination_ch_table='tmp.xinghuan_mc_xdmp_shop',
    aggregate_query=None,
    dag=dag
)


def ch_transfer():
    pass


transfer_ch_data_tmp_to_dim = PythonOperator(
    task_id='transfer_ch_data_tmp_to_dim',
    python_callable=ch_transfer,
    op_kwargs=None,
    dag=dag
)

ch_xdqc_tb_shop_to_pulsar = ClickHouseToPulsarOperator(
    task_id='ch_xdqc_tb_shop_to_pulsar',
    ch_conn_id=CH_CONN_ID,
    ch_query_sql='SELECT * FROM pub_app.shop_overview_all where day = {{ ds_nodash }}',
    pulsar_conn_id=PULSAR_CONN_ID,
    topic=PULSAR_TOPIC,
    header=header,
    dag=dag
)

ch_xinghuan_mc_company_shop_to_pulsar = ClickHouseToPulsarOperator(

)

truncate_ch_tmp_table >> [
    mongo_xdqc_tb_shop_to_ch_tmp, mongo_xinghuan_mc_company_shop_to_ch_tmp,
    mongo_xinghuan_mc_company_to_ch_tmp, mongo_xinghuan_mc_tmp_xdmp_shop_to_ch_tmp
] >> transfer_ch_data_tmp_to_dim
