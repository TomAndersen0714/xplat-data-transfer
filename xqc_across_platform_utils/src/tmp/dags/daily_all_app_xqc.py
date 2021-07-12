from airflow import DAG
from datetime import timedelta
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.mongo_to_clickhouse_operator import MongoToClickHouseOperator
# from airflow.contrib.hooks.pulsar_send_hook import PulsarSendHook
from src.tmp.hooks.pulsar_hook import PulsarSendHook

from datetime import datetime

default_args = {
    'owner': 'yangboxiao',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 15),
    'email': ['yangboxiao@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG('daily_all_app_xqc', default_args=default_args, schedule_interval="30 5 * * *", max_active_runs=1,
          concurrency=2)
CH_HOOK = 'clickhouse_v1mini-bigdata-002'


# 清空CH中的指定表
def truncate_ch_table(tbl_name):
    ch = ClickHouseHook(CH_HOOK)
    ch.execute(f'truncate table {tbl_name} on cluster cluster_3s_2r ')


# 清空 ods.cut_word_qid_local 表
truncate_xqc_kefu_stat = PythonOperator(
    task_id="truncate_xqc_kefu_stat",
    python_callable=truncate_ch_table,
    op_kwargs={"tbl_name": "tmp.xqc_kefu_stat_local"},
    dag=dag
)


def gen_agg_query(ds_nodash):
    return [
        {"$match": {
            "date": {
                "$gte": int(ds_nodash)}
        }
        }
    ]


xdqc_kefu_statl_to_ch_ods = MongoToClickHouseOperator(
    task_id='xdqc_kefu_statl_to_ch_ods',
    mongo_conn_id='xdqc_mongo',
    clickhouse_conn_id=CH_HOOK,
    mongo_db='xdqc-tb',
    mongo_collection='kefu_stat',
    destination_ch_table='tmp.xqc_kefu_stat_all',
    aggregate_query=None,
    aggregate_func=gen_agg_query,
    aggregate_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    dag=dag
)

TestConf = {
    "service_url": "http://10.22.112.10:81/mini-data-sync/send?type=bigdata",
    "database_url": "http://v1mini-bigdata-003:14000",
    "worekr_name": "xqc_kefu_stat",
    "done_files_dir": "/data2/code_workplace/cross_platform",
    "output_dir": "/data2/code_workplace/cross_platform_out",
    "pulsar_url": "pulsar://pulsar-cluster01-slb:6650",
    "topic": "persistent://bigdata/data_cross/mini_send_tb",
    "export_cmd": "docker exec d5 bash -c 'clickhouse-client --port=19000 --query  \"SELECT * FROM tmp.xqc_kefu_stat_all FORMAT Parquet\"'"
}


def out_func(ds_nodash, conf):
    print(conf)
    send_hook = PulsarSendHook(conf)
    file_list = send_hook.get_done_files()
    for file in file_list:
        send_hook.do_file(file, ds_nodash)


test_out_task = PythonOperator(
    task_id="test_out_task",
    python_callable=out_func,
    op_kwargs={
        "ds_nodash": "{{ ds_nodash }}",
        "conf": TestConf
    },
    dag=dag
)

truncate_xqc_kefu_stat >> xdqc_kefu_statl_to_ch_ods >> test_out_task