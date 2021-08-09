from time import sleep
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from airflow.contrib.operators.mongo_to_clickhouse_operator import MongoToClickHouseOperator
from airflow.operators.python_operator import PythonOperator

MONGO_CONN_ID = 'xdqc_mongo'
CH_CONN_ID = 'clickhouse_zjk_008'

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 29),
    'email': ['chengcheng@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='xqc_dim_table_sync_to_tb',
    description='xqc维度表T+1同步',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)


def truncate_table_on_cluster(tbl):
    ch_hook = ClickHouseHook(CH_CONN_ID)
    ch_hook.truncate_table(tbl)


def timestamp_mapper(k, v):
    if isinstance(v, datetime) and v > datetime(1970, 1, 1, 8):
        return [(str(k), int(v.timestamp() * 1000))]
    else:
        return [(str(k), 0)]


def add_extra_column(d):
    d['source'] = 'tb'


truncate_shop_table = PythonOperator(
    task_id='truncate_shop_table',
    python_callable=truncate_table_on_cluster,
    op_kwargs={
        'tbl': 'xqc_dim.shop_local'
    },
    dag=dag
)

xdqc_tb_shop_mongo_to_ch = MongoToClickHouseOperator(
    task_id='xdqc_tb_shop_mongo_to_ch',
    mongo_conn_id=MONGO_CONN_ID,
    mongo_db='xdqc-tb',
    mongo_collection='shop',
    clickhouse_conn_id=CH_CONN_ID,
    destination_ch_table='xqc_dim.shop_all',
    aggregate_query=None,
    flatten_map=add_extra_column,
    dag=dag
)

# truncate_xinghuan_mc_company_shop_local = PythonOperator(
#     task_id='truncate_xinghuan_mc_company_shop_local',
#     python_callable=truncate_table_on_cluster,
#     op_kwargs={
#         'tbl': 'xqc_dim.xinghuan_mc_company_shop_local'
#     },
#     dag=dag
# )
#
# xinghuan_mc_company_shop_mongo_to_ch = MongoToClickHouseOperator(
#     task_id='xinghuan_mc_company_shop_mongo_to_ch',
#     mongo_conn_id=MONGO_CONN_ID,
#     mongo_db='xinghuan-mc',
#     mongo_collection='company_shop',
#     clickhouse_conn_id=CH_CONN_ID,
#     destination_ch_table='xqc_dim.xinghuan_mc_company_shop_all',
#     aggregate_query=None,
#     flatten_map=add_extra_column,
#     dag=dag
# )
#
# truncate_xinghuan_mc_company_local = PythonOperator(
#     task_id='truncate_xinghuan_mc_company_local',
#     python_callable=truncate_table_on_cluster,
#     op_kwargs={
#         'tbl': 'xqc_dim.xinghuan_mc_company_local'
#     },
#     dag=dag
# )
#
# xinghuan_mc_company_mongo_to_ch = MongoToClickHouseOperator(
#     task_id='xinghuan_mc_company_mongo_to_ch',
#     mongo_conn_id=MONGO_CONN_ID,
#     mongo_db='xinghuan-mc',
#     mongo_collection='company',
#     clickhouse_conn_id=CH_CONN_ID,
#     destination_ch_table='xqc_dim.xinghuan_mc_company_all',
#     aggregate_query=None,
#     flatten_map=add_extra_column,
#     dag=dag
# )
#
# truncate_xinghuan_mc_xdmp_shop_local = PythonOperator(
#     task_id='truncate_xinghuan_mc_xdmp_shop_local',
#     python_callable=truncate_table_on_cluster,
#     op_kwargs={
#         'tbl': 'xqc_dim.xinghuan_mc_xdmp_shop_local'
#     },
#     dag=dag
# )
#
# xinghuan_mc_xdmp_shop_mongo_to_ch = MongoToClickHouseOperator(
#     task_id='xinghuan_mc_xdmp_shop_mongo_to_ch',
#     mongo_conn_id=MONGO_CONN_ID,
#     mongo_db='xinghuan-mc',
#     mongo_collection='tmp_xdmp_shop',
#     clickhouse_conn_id=CH_CONN_ID,
#     destination_ch_table='xqc_dim.xinghuan_mc_xdmp_shop_all',
#     aggregate_query=None,
#     flatten_map=add_extra_column,
#     dag=dag
# )

truncate_shop_table >> xdqc_tb_shop_mongo_to_ch
# truncate_xinghuan_mc_company_local >> xinghuan_mc_company_mongo_to_ch
# truncate_xinghuan_mc_company_shop_local >> xinghuan_mc_company_shop_mongo_to_ch
# truncate_xinghuan_mc_xdmp_shop_local >> xinghuan_mc_xdmp_shop_mongo_to_ch
