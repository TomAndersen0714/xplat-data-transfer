from time import sleep

from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
# from airflow.contrib.hooks.pulsar_send_hook import PulsarHook
from hooks.pulsar_hook import PulsarHook
from airflow.contrib.operators.mongo_to_clickhouse_operator import MongoToClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'chengcheng',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 15),
    'email': ['chengcheng@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': timedelta(minutes=5),
}

# Configuration
mini_ch_conn_id = 'clickhouse_v1mini-bigdata-002'
mini_ch_container_id = 'd57091c972e7'
mini_ch_data_export_cmd = ''

ch_tmp_local_tab = 'tmp.xdqc_kefu_stat_daily_local'
ch_tmp_dist_tab = 'tmp.xdqc_kefu_stat_daily_all'
ch_dest_tab = 'xqc_ods.xdqc_kefu_stat_all'

msg_header = {
    "pulsar_url": "pulsar://pulsar-cluster01-slb:6650",
    "topic": "persistent://bigdata/data_cross/BI_xdqc_kefu_stat_daily",
    "worker_name": "xdqc_kefu_stat_daily",
    "date": "{{ ds_nodash }}",
    "tmp_local_tab": ch_tmp_local_tab,
    "tmp_dist_tab": ch_tmp_dist_tab,
    "dest_tab": ch_dest_tab
}

dag = DAG(
    dag_id='xdqc_kefu_stat_daily',
    description='MongoDB:xdqc-tb.kefu_stat增量同步到小程序CH以及跨平台发送到老淘宝集群',
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)


# 清空 ClickHouse 指定表
def truncate_ch_table(tbl_name):
    ch = ClickHouseHook(mini_ch_conn_id)
    ch.truncate_table(tbl_name)
    sleep(3)


# 清空 ClickHouse:tmp.xqc_kefu_stat_local 表
truncate_ch_xdqc_kefu_stat_daily = PythonOperator(
    task_id="truncate_xdqc_kefu_stat_daily",
    python_callable=truncate_ch_table,
    op_kwargs={"tbl_name": ch_tmp_local_tab},
    dag=dag
)


# 设定 MongoDB 查询ETL条件: 增量查询
def gen_agg_query(ds_nodash):
    return [
        {"$match": {
            "date": {
                "$gte": int(ds_nodash)}
        }
        }
    ]


# 查询 MongoDB 将ETL后数据导入 Mini ClickHouse 转换为目标格式
xdqc_kefu_stat_daily_to_ch = MongoToClickHouseOperator(
    task_id='MongoDB_xdqc-tb.kefu_stat_to_CH_delta_daily',
    mongo_conn_id='xdqc_mongo',
    clickhouse_conn_id=mini_ch_conn_id,
    mongo_db='xdqc-tb',
    mongo_collection='kefu_stat',
    destination_ch_table=ch_tmp_dist_tab,
    aggregate_query=None,
    aggregate_func=gen_agg_query,
    aggregate_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    rows_chunk=100000,
    dag=dag
)


# 将 ClickHouse 指定表查询结果写入特定表
def flush_ch_data(source_tab, dest_tab):
    ch_hook = ClickHouseHook(mini_ch_conn_id)
    ch_sql = f'INSERT INTO {dest_tab} SELECT * FROM {source_tab}'
    ch_hook.execute(ch_sql)
    sleep(3)


# 将 ClickHouse 增量临时表内容写入全量表
flush_ch_tmp_data_to_all_daily = PythonOperator(
    task_id="flush_xdqc_kefu_stat_tmp_to_all_daily",
    python_callable=flush_ch_data,
    op_kwargs={
        "source_tab": ch_tmp_dist_tab,
        "dest_tab": ch_dest_tab
    },
    dag=dag
)


# 导出 ClickHouse 查询结果(Parquet)到CH服务器端
def export_ch_data_to_local(tab_name):

    pass

# 导出 ClickHouse:tmp.xdqc_kefu_stat_daily_local
export_xdqc_kefu_stat_daily_local = PythonOperator(
    task_id="export_xdqc_kefu_stat_daily_local_to_local",
    python_callable=export_ch_data_to_local,
    dag=dag
)

# 按天发布 tmp.xdqc_kefu_stat_daily_all 数据到跨平台消息队列集群
# 便于其他平台按需订阅和使用
publish_xdqc_kefu_stat_daily = PythonOperator(

)

# 远程调用

truncate_ch_xdqc_kefu_stat_daily >> xdqc_kefu_stat_daily_to_ch >> \
flush_ch_tmp_data_to_all_daily >> publish_xdqc_kefu_stat_daily
