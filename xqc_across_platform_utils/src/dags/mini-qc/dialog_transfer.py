from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from clickhouse_driver import Client
from airflow.contrib.operators.mongo_to_clickhouse_operator import MongoToClickHouseOperator
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.bash_operator import BashOperator
from pymongo import MongoClient
import time

args = {
    'owner': 'yangboxiao',
    'start_date': datetime(2021, 2, 23),
    'email': ['yangboxiao@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='dialog_transfer',
    schedule_interval="30 6 * * *",
    max_active_runs=1,
    default_args=args
)


def truncate_ch_tmp_table_func():
    ch_hook = ClickHouseHook("clickhouse")
    ch_hook.execute(f"truncate table tmp.xinghuan_department")
    ch_hook.execute(f"truncate table tmp.xinghuan_company_snick")
    ch_hook.execute(f"truncate table tmp.xinghuan_qc_task")
    ch_hook.execute(f"truncate table tmp.xinghuan_qc_task_instance")


truncate_ch_tmp_table = PythonOperator(
    task_id="truncate_ch_tmp_table",
    python_callable=truncate_ch_tmp_table_func,
    dag=dag
)


def qc_task_detail_func(yesterday_ds, yesterday_ds_nodash, ds, ds_nodash):
    # 质检任务详情 step3
    ch_hook = ClickHouseHook("clickhouse")
    ch_hook.execute(
        f"alter table ods.xinghuan_qc_task_detail_all drop partition {ds_nodash}")
    load_xinghuan_qc_task_detail_sql = f"""insert into ods.xinghuan_qc_task_detail_all
    select
        toDate('{ds}') as `date`,  
        platform,
        task_id,
        task_table.company_id,
        snick_table.department_id,
        snick_table.department_name,
        task_table.qc_norm_id,
        task_table.qc_norm_name, 
        account_id,
        username,
        dialog_id,
        is_mark,
        date, 
        seller_nick,
        task_table.snick,
        snick_table.employee_name,
        snick_table.superior_name,
        cnick,
        begin_time,
        end_time,
         order_id,
         mark,
        mark_score,
        mark_score_add,
        score,
        score_add,
        abnormals_type ,
         abnormals_count,
         excellents_type,
        excellents_count,
         s_emotion_type,
         s_emotion_count, 
         c_emotion_type,
         c_emotion_count,
         tag_score_stats_id,
         tag_score_add_stats_id
        from (
        select 
        platform,
        qc_task_id as task_id,
        company_id,
        department_id,
        department_name,
        qc_norm_id,
        qc_norm_name, 
        account_id,
        username,
        dialog_id,
        is_mark,
        date, 
        seller_nick,
        snick,
        cnick,
        begin_time,
        end_time,
         order_id,
         mark,
        mark_score,
        mark_score_add,
        score,
        score_add,
        abnormals_type ,
         abnormals_count,
         excellents_type,
         excellents_count,
        s_emotion_type,
         s_emotion_count, 
         c_emotion_type,
         c_emotion_count,
        tag_score_stats_id,
        tag_score_add_stats_id
        from ( select _id,seller_nick,snick,cnick,begin_time,end_time,order_info_id[1] as order_id,
        mark,
        mark_score,
        mark_score_add,
        score,
        score_add,
        abnormals_type,
        abnormals_count,
        excellents_type,
        excellents_count,
        s_emotion_type,
        s_emotion_count, 
        c_emotion_type,
        c_emotion_count,
        tag_score_stats_id,
        tag_score_add_stats_id
        from dwd.xdqc_dialog_all where toYYYYMMDD(begin_time) ={yesterday_ds_nodash} )  as dialog_all
        left join 
        (
        select  
        platform,
        qc_task_id,
        company_id,
        department_id,
        department_name,
        qc_norm_id,
        qc_norm_name, 
        account_id,
        username,
        dialog_id,
        is_mark,
        date
        from (select   
        platform,
        _id,
        company_id,
        department_id,
        department_name,
        qc_norm_id,
        qc_norm_name, 
        account_id,
        b.username
        from (
        select 
         platform,
        _id,
        company_id,
        department_id,
        department_name,
        qc_norm_id,
        b.name as qc_norm_name, 
        account_id
        from  
        (
        select platform,
        _id,
        company_id,
        department_id,
        b.name  as department_name,
        qc_norm_id,
        account_id from 
        (
        select 
        platform,
        _id,
        company_id,
        department_id,
        qc_norm_id,
        account_id
        from ods.xinghuan_qc_task_all where day = {ds_nodash}) as a 
        left join(
        select _id,name from  ods.xinghuan_department_all  where day = {ds_nodash}
        ) as b 
        on 
        a.department_id = b._id 
        ) as a 
        left join (
        select _id , name from   ods.xinghuan_qc_norm_all  where day = {ds_nodash}
        ) as b 
        on a.qc_norm_id = b._id 
        ) as a 
        left join 
        (
            select a.account_id as account_id,
        a.company_id as company_id,
        a.employee_id as  employee_id,
        e.employee_name as username
        from 
        (select  _id as account_id,employee_id,company_id,username from ods.xinghuan_account_all where day = {ds_nodash} ) as a 
        left join 
        (select company_id,department_id,_id as employee_id , superior_id, superior_name,username as  employee_name from  ods.xinghuan_employee_all where day = {ds_nodash} ) as e 
        on a.employee_id = e.employee_id
        and a.company_id=e.company_id
        ) as b 
        on a.account_id = b.account_id ) as task_info
        left join (
        select qc_task_id,dialog_id,is_mark,toYYYYMMDD(toDate(toInt32(date))) as date from ods.xinghuan_qc_task_instance_all  where day = {ds_nodash} and date = {ds_nodash} 
        ) as instanc_info
        on task_info._id =instanc_info.qc_task_id 
        ) as task_info_all
        on task_info_all.dialog_id = dialog_all._id
        ) as task_table
        left join 
        (
        select 
                department_info.company_id as company_id,
                department_info.department_id as department_id,
                department_info.department_name as department_name,
                department_info.employee_id as employee_id,
                department_info.superior_id as superior_id,
                department_info.superior_name as superior_name,
                department_info.employee_name as employee_name,
                department_info.snick as snick,
                account.account_id as account_id
         from (
         select 
        a.company_id,
        a.department_id,
        b.department_name,
        a.employee_id,
        a.superior_id,
        a.superior_name,
        a.employee_name,
        a.snick
        from 
        (select 
        a.company_id,
        a.department_id,
        a.employee_id,
        a.superior_id,
        a.superior_name,
        a.employee_name,
        b.snick from (
        select company_id,department_id,_id as employee_id , superior_id, superior_name,username as  employee_name from  ods.xinghuan_employee_all where day = {ds_nodash}
        ) as a 
        left join 
        (
        select platform,company_id,employee_id,snick from ods.xinghuan_employee_snick_all  where day = {ds_nodash} and platform ='jd' 
        ) as b 
        on a.employee_id = b. employee_id
        and a.company_id =b.company_id 
        ) as a 
        left join (
        select _id,name as department_name from ods.xinghuan_department_all where day = {ds_nodash}
        ) as b 
        on a.department_id = b._id 
         ) as department_info 
        left join 
        (
        select a.account_id as account_id,
        a.company_id as company_id,
        a.employee_id as  employee_id,
        e.employee_name as username
        from 
        (select  _id as account_id,employee_id,company_id,username from ods.xinghuan_account_all where day = {ds_nodash} ) as a 
        left join 
        (select company_id,department_id,_id as employee_id , superior_id, superior_name,username as  employee_name from  ods.xinghuan_employee_all where day = {ds_nodash} ) as e 
        on a.employee_id = e.employee_id
        and a.company_id=e.company_id
        ) as account 
        on department_info.company_id = account.company_id
        and department_info.employee_id = account.employee_id
        ) as snick_table 
        on task_table.company_id =snick_table.company_id
        and  task_table.snick = snick_table.snick
        """
    ch_hook.execute(load_xinghuan_qc_task_detail_sql)
    # ods.xinghuan_qc_task_all
    # ods.xinghuan_department_all
    # ods.xinghuan_qc_norm_all
    # ods.xinghuan_account_all
    # ods.xinghuan_employee_all
    # ods.xinghuan_employee_snick_all


def load_data_tmp2ods_func(ds_nodash):
    ch_hook = ClickHouseHook("clickhouse")
    ch_hook.execute(f"alter table ods.xinghuan_department_all  drop partition {ds_nodash}")
    ch_hook.execute(f"alter table ods.xinghuan_company_snick_all  drop partition {ds_nodash}")
    ch_hook.execute(f"alter table ods.xinghuan_qc_task_all  drop partition {ds_nodash}")
    ch_hook.execute(
        f"alter table ods.xinghuan_qc_task_instance_all  drop partition {ds_nodash}")

    # 加载日期分区数据
    load_ods_xinghuan_department = f"""insert into ods.xinghuan_department_all select *,{ds_nodash} as `day` from tmp.xinghuan_department;"""
    load_ods_xinghuan_company_snick = f"""insert into ods.xinghuan_company_snick_all select *,{ds_nodash} as `day` from tmp.xinghuan_company_snick;"""
    load_ods_xinghuan_qc_task = f"""insert into ods.xinghuan_qc_task_all select *,{ds_nodash} as `day` from tmp.xinghuan_qc_task;"""
    load_ods_xinghuan_qc_task_instance = f"""insert into ods.xinghuan_qc_task_instance_all select *,{ds_nodash} as `day` from tmp.xinghuan_qc_task_instance;"""

    ch_hook.execute(load_ods_xinghuan_department)
    ch_hook.execute(load_ods_xinghuan_company_snick)
    ch_hook.execute(load_ods_xinghuan_qc_task)
    ch_hook.execute(load_ods_xinghuan_qc_task_instance)


load_data_tmp2ods = PythonOperator(
    task_id="load_data_tmp2ods",
    python_callable=load_data_tmp2ods_func,
    op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    dag=dag
)

# 从mongo里面拉取xinghuan-mc.department信息数据到ch
sync_xinghuan_department_to_ch_tmp = MongoToClickHouseOperator(
    task_id='sync_xinghuan_department_to_ch_tmp',
    mongo_conn_id='xdqc_mongo',
    clickhouse_conn_id='clickhouse',
    mongo_db='xinghuan-mc',
    mongo_collection='department',
    destination_ch_table='tmp.xinghuan_department',
    aggregate_query=None,
    flatten_map=None,
    dag=dag
)

sync_xinghuan_company_snick_ch_tmp = MongoToClickHouseOperator(
    task_id='sync_xinghuan_company_snick_ch_tmp',
    mongo_conn_id='xdqc_mongo',
    clickhouse_conn_id='clickhouse',
    mongo_db='xinghuan-mc',
    mongo_collection='employee',
    destination_ch_table='tmp.xinghuan_company_snick',
    aggregate_query=None,
    flatten_map=None,
    dag=dag
)

sync_xinghuan_qc_task_to_ch_tmp = MongoToClickHouseOperator(
    task_id='sync_xinghuan_qc_task_to_ch_tmp',
    mongo_conn_id='xdqc_mongo',
    clickhouse_conn_id='clickhouse',
    mongo_db='xinghuan-mc',
    mongo_collection='qc_task',
    destination_ch_table='tmp.xinghuan_qc_task',
    aggregate_query=None,
    flatten_map=None,
    dag=dag
)
sync_xinghuan_qc_task_instance_to_ch_tmp = MongoToClickHouseOperator(
    task_id='sync_xinghuan_qc_task_instance_to_ch_tmp',
    mongo_conn_id='xdqc_mongo',
    clickhouse_conn_id='clickhouse',
    mongo_db='xinghuan-mc',
    mongo_collection='qc_task_instance',
    destination_ch_table='tmp.xinghuan_qc_task_instance',
    aggregate_query=None,
    flatten_map=None,
    dag=dag
)
qc_task_detail = PythonOperator(
    task_id="qc_task_detail",
    python_callable=qc_task_detail_func,
    op_kwargs={
        "ds": "{{ ds }}",
        "ds_nodash": "{{ ds_nodash }}",
        "yesterday_ds": "{{ yesterday_ds }}",
        "yesterday_ds_nodash": "{{ yesterday_ds_nodash }}"
    },
    dag=dag
)
truncate_ch_tmp_table >> [sync_xinghuan_department_to_ch_tmp,
                          sync_xinghuan_qc_task_to_ch_tmp,
                          sync_xinghuan_qc_task_instance_to_ch_tmp,
                          sync_xinghuan_company_snick_ch_tmp
                          ] >> load_data_tmp2ods >> qc_task_detail

