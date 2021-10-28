import uuid

from airflow import DAG
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.contrib.operators.clickhouse_to_pulsar import ClickHouseToPulsarOperator
from datetime import datetime, timedelta

# 基础配置
dag_id = 'demo_id'  # 自定义
source_platform = 'jd'
dest_platform = 'tb'
batch_id = str(uuid.uuid4())

# 数据源
ch_conn_id = f'clickhouse_{source_platform}'
ch_tmp_dist_table = 'tmp.xdqc_kefu_stat_daily_all'

# 消息队列
pulsar_conn_id = 'pulsar_cluster01_slb'
pulsar_topic = f'persistent://bigdata/data_cross/{source_platform}_send_{dest_platform}'

# 源表、目标表、清除表
ch_source_table = 'tmp.truncate_test_all'
ch_dest_table_1 = 'tmp.truncate_test_all'
ch_clear_table_1 = 'tmp.truncate_test_local'

ch_dest_table_2 = 'tmp.truncate_test_all_1'
ch_clear_table_2 = 'tmp.truncate_test_local_1'

ch_dest_table_3 = 'tmp.drop_partition_test_all'
ch_clear_table_3 = 'tmp.drop_partition_test_local'

# header demo
header1 = PulsarHook.get_ch_msg_header(
    task_id="ch_to_pulsar_test",  # 可选项
    source_table=ch_source_table,  # 数据源表
    target_table=ch_dest_table_1,  # 需要写入的 buffer 或 distributed 表
    clear_table=ch_clear_table_1,  # 需要清除的local表
    partition="(2021,7,13)",  # 数值字段组成的分区键表达式, 直接使用元组字符串形式,如果需要使用Airflow宏参数,请务必不要直接使用f"{{ds_nodasj}}"的方式进行格式化
    cluster_name="cluster_3s_2r",  # 集群配置有分区名则写分区名, 未配置则配置为空字符串"",或者删除此字段
    batch_id=batch_id  # 可选项
)

# 分区幂等1
header2 = PulsarHook.get_ch_msg_header(
    task_id="ch_to_pulsar_test",  # 可选项
    source_table=ch_source_table,
    target_table=ch_dest_table_2,
    clear_table=ch_clear_table_2,
    partition="(2021,'Tom','Andersen')",  # 多个分区键组成的分区表达式, 使用元组字符串形式, 其中的字符串字段使用单引号标识
    cluster_name="cluster_3s_2r",
    batch_id=batch_id  # 可选项
)

# 分区幂等2
header3 = PulsarHook.get_ch_msg_header(
    task_id='test',  # 可选项
    source_table=ch_source_table,
    target_table=ch_dest_table_3,
    clear_table=ch_clear_table_3,
    partition="'jd'",  # 单个分区键,如果是字符串类型需要加上单引号与数值类型进行区分
    cluster_name="",
    batch_id=batch_id  # 可选项
)

# 整表幂等
header4 = PulsarHook.get_ch_msg_header(
    task_id='test',  # 可选项
    source_table=ch_source_table,
    target_table=ch_dest_table_3,
    clear_table=ch_clear_table_3,  # 只定义clear_table字段,不定义partition,则每次写入之前truncate整张表
    cluster_name="cluster_3s_2r",
    batch_id=batch_id  # 可选项
)

default_args = {
    'owner': 'xiaoduo',  # 自定义
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 14),  # 自定义
    'email': ['demo@xiaoduotech.com'],  # 自定义
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=dag_id,
    description='ch_to_pulsar_test',  # 自定义
    default_args=default_args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)

# fetch data from clickhouse and send to pular
ch_to_pulsar_test_1 = ClickHouseToPulsarOperator(
    task_id='ch_to_pulsar_test_1',
    ch_conn_id=ch_conn_id,
    ch_query_sql=f'SELECT * FROM {ch_source_table}',  # 查询SQL可以自定义, 务必保证查询结果和目标表字段名完全相同, 各个对应字段之间类型转换自行控制
    pulsar_conn_id=pulsar_conn_id,
    topic=pulsar_topic,
    header=header1,
    dag=dag
)
