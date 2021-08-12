from airflow.contrib.operators.impala_executor import ImpalaOperator
from airflow import DAG
from airflow.contrib.hooks.pulsar_hook import PulsarHook
from airflow.contrib.operators.impala_to_pulsar import ImpalaToPulsarOperator
from datetime import datetime, timedelta

# 基础配置
source_platform = 'jd'
target_platform = 'tb'
target_db = 'kudu'
job_id = 'dipper'
interval = 'daily'
dag_id = f'{job_id}_{source_platform}_to_{target_platform}_{target_db}_{interval}'

# 数据源
imp_conn_id = 'impala_001'
imp_source_table = 'dipper_ods.shop_overview_day'

# 数据管道
pulsar_conn_id = 'pulsar_cluster01_slb'
pulsar_topic = f'persistent://bigdata/data_cross/{source_platform}_send_{target_platform}'

# 目标表
kudu_target_table_1 = 'impala::dipper_ods.shop_overview_day'
kudu_target_table_2 = 'impala::dipper_ods.reminder_shop_stat_day'

args = {
    'owner': 'songzhen',
    'start_date': datetime(2021, 8, 11, 7),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}
dag = DAG(
    dag_id=dag_id,
    description='impala_to_kudu_test',
    default_args=args,
    schedule_interval="30 5 * * *",
    max_active_runs=1,
    concurrency=2
)

# shop统计大宽表
dipper_ods_shop_overview_day_sql = """
    SELECT 
        shop_id,
        'jd' as platform,
        `day`,
        kb_send_num,
        kb_paid_buyer_num,
        kb_paid_order_num,
        kb_paid_order_payment,
        sm_recv_cnt,
        sm_recv_uv,
        sm_idfy_cnt,
        sm_trans_uv,
        sm_repl_cnt,
        hm_recv_cnt,
        hm_recv_uv,
        hm_idfy_cnt,
        hm_repl_cnt,
        recv_cnt,
        recv_uv,
        idfy_cnt,
        repl_cnt,
        use_account_cnt,
        order_pemt_amt,
        rs_human_avg_resp_interval,
        rs_hci_avg_resp_interval,
        rs_reduce_interval,
        rs_human_save_hours,
        rs_human_work_ratio
    FROM dipper_ods.shop_overview_day 
    -- where day={{ds_nodash}}
"""

shop_overview_header = PulsarHook.get_kudu_msg_header(
    task_id="dipper_ods_shop_overview_day_imp2kudu",
    source_table=imp_source_table,
    target_table=kudu_target_table_1,
    source_platform=source_platform,
    target_platform=target_platform
)

dipper_ods_shop_overview_day_imp2kudu = ImpalaToPulsarOperator(
    task_id='dipper_ods_shop_overview_day_imp2kudu_job',
    imp_conn_id=imp_conn_id,
    pulsar_conn_id=pulsar_conn_id,
    imp_sql=dipper_ods_shop_overview_day_sql,
    topic=pulsar_topic,
    header=shop_overview_header,
    dag=dag
)

# reminder_shop_stat 统计大宽表
dipper_reminder_shop_stat_day_sql = """
    SELECT 
        day,
        shop_id,
        node_type,
        customer_type,
        shop_name,
        'jd' as platform,
        send_count,
        send_buyers_count,
        reply_buyers_count,
        reply_rate,
        conversion_buyers_count,
        conversion_order_count,
        conversion_rate,
        conversion_payment
    FROM app_mp.reminder_shop_stat
"""

reminder_shop_stat_header = PulsarHook.get_kudu_msg_header(
    task_id="dipper_ods_reminder_shop_stat_day_imp2kudu",
    source_table=imp_source_table,
    target_table=kudu_target_table_2,
    source_platform=source_platform,
    target_platform=target_platform
)

dipper_ods_reminder_shop_stat_day_imp2kudu = ImpalaToPulsarOperator(
    task_id='dipper_ods_reminder_shop_stat_day_imp2kudu_job',
    imp_conn_id=imp_conn_id,
    pulsar_conn_id=pulsar_conn_id,
    imp_sql=dipper_reminder_shop_stat_day_sql,
    topic=pulsar_topic,
    header=reminder_shop_stat_header,
    dag=dag
)

dipper_ods_shop_overview_day_imp2kudu >> dipper_ods_reminder_shop_stat_day_imp2kudu
