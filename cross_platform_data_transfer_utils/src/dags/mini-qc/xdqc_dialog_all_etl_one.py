import logging as log
import time

from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.python_operator import PythonOperator
from clickhouse_driver import Client
from pymongo import MongoClient

MONGO_URL = "mongodb://root:CDxddev2189@dds-k2je4842705df3e41.mongodb.zhangbei.rds.aliyuncs.com:3717," \
            "dds-k2je4842705df3e42.mongodb.zhangbei.rds.aliyuncs.com:3717/admin?replicaSet=mgset-500111166"
MONGO_DB = "xdqc"
MONGO_COLLECTION = "dialog"

CH_HOST = "10.20.2.29"
CH_PORT = 19000

CHUNK_SIZE = 100000

args = {
    'owner': 'shenyong',
    'start_date': datetime(2020, 11, 16),
    'email': ['shenyong@xiaoduotech.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0
}

dag = DAG(
    dag_id='xdqc_dialog_all_etl_one',
    schedule_interval="30 2 * * *",
    max_active_runs=1,
    default_args=args
)

FIELDS = {
    "_id": "string",
    "last_mark_id": "string",
    "task_list_id": "string",
    "last_msg_id": "string",
    "platform": "string",
    "channel": "string",
    "cnick": "string",
    "snick": "string",
    "seller_nick": "string",
    "mark": "string",
    "employee_name": "string",
    "focus_goods_id": "string",
    "group": "string",
    "mark_judge": "int",
    "mark_score": "int",
    "mark_score_add": "int",
    "score": "int",
    "score_add": "int",
    "qa_time_sum": "int",
    "qa_round_sum": "int",
    "emotion_detect_mode": "int",
    "consulte_transfor_v2": "int",
    "intel_score": "int",
    "remind_ntype": "int",
    "date": "int",
    "first_answer_msg_type": "int",
    "answer_count": "int",
    "question_count": "int",
    "first_answer_time": "int",
    "is_after_sale": "bool",
    "is_inside": "bool",
    "human_check": "bool",
    "has_withdraw_robot_msg": "bool",
    "is_remind": "bool",
    "is_order_matched": "bool",
    "suspected_positive_emotion": "bool",
    "is_follow_up_remind": "bool",
    "suspected_problem": "bool",
    "suspected_excellent": "bool",
    "has_after": "bool",
    "begin_time": "timestamp",
    "end_time": "timestamp",
    "first_follow_up_time": "timestamp",
    "mark_ids": "array_string",
    "read_mark": "array_string",
    "cnick_customize_rule": "array_string",
    "emotions": "array_string",
    "qid": "array_int",
    "tag_score_stats": "tag_score_stats",
    "tag_score_add_stats": "tag_score_stats",
    "rule_stats": "rule_stats",
    "rule_add_stats": "rule_stats",
    "s_emotion": "emotion",
    "c_emotion": "emotion",
    "abnormals": "ab_normal",
    "excellents": "excellent",
    "qc_word": "qc_word",
    "order_info": "order_info",
    "update_time": "timestamp",
}


def u_int(i):
    if i < 0:
        return 0
    else:
        return i


def int_mapper(*args):
    k = args[0]
    v = args[1]

    return [(str(k), int(v) if v is not None else 0)]


def string_mapper(*args):
    k = args[0]
    v = args[1]

    return [(str(k), str(v) if v is not None else "")]


def bool_mapper(*args):
    k = args[0]
    v = args[1]

    return [(str(k), bool(v) if v is not None else False)]


def array_int_mapper(*args):
    k = args[0]
    v = args[1]

    if isinstance(v, list) and v is not None:
        return [(str(k), [int(e) for e in v])]
    else:
        return [(str(k), [])]


def array_string_mapper(*args):
    k = args[0]
    v = args[1]

    if isinstance(v, list) and v is not None:
        return [(str(k), [str(e) for e in v])]
    else:
        return [(str(k), [])]


def timestamp_mapper(*args):
    k = args[0]
    v = args[1]

    if isinstance(v, datetime) and v > datetime(1970, 1, 1, 8):
        v += timedelta(hours=8)
        if k == 'update_time':
            return [(str(k), int(v.timestamp()))]
        else:
            return [(str(k), int(v.timestamp() * 1000))]
    else:
        return [(str(k), 0)]


def tag_score_stats_mapper(*args):
    k = args[0]
    v = args[1]

    ids = []
    scores = []

    if isinstance(v, list) and v is not None:
        for s in v:
            ids.append(str(s.get("id", "")))
            scores.append(int(s.get("score", 0)))

    return [(f"{k}.id", ids), (f"{k}.score", scores)]


def rule_stats_mapper(*args):
    k = args[0]
    v = args[1]

    ids = []
    scores = []
    counts = []

    if isinstance(v, list) and v is not None:
        for s in v:
            ids.append(str(s.get("id", "")))
            scores.append(int(s.get("score", 0)))
            counts.append(u_int(int(s.get("count", 0))))

    return [(f"{k}.id", ids), (f"{k}.score", scores), (f"{k}.count", counts)]


def type_count_mapper(*args):
    k = args[0]
    v = args[1]

    types = []
    counts = []

    if isinstance(v, list) and v is not None:
        for s in v:
            types.append(int(s.get("type", -1)))
            counts.append(u_int(int(s.get("count", 0))))

    return [(f"{k}.type", types), (f"{k}.count", counts)]


def qc_word_mapper(*args):
    k = args[0]
    v = args[1]

    sources = []
    words = []
    counts = []

    if isinstance(v, list) and v is not None:
        for e in v:
            sources.append(int(e.get("source", 0)))
            words.append(str(e.get("word", "")))
            counts.append(u_int(int(e.get("count", 0))))

    return [(f"{k}.source", sources), (f"{k}.word", words), (f"{k}.count", counts)]


def order_info_mapper(*args):
    k = args[0]
    v = args[1]

    order_info_id = []
    order_info_status = []
    order_info_payment = []
    order_info_time = []

    if v is not None:
        order_info_id.append(str(v.get("order_id", "")))
        order_info_status.append(str(v.get("status", "")))
        order_info_payment.append(float(v.get("payment", 0.0)))
        order_info_time.append(int(v.get("order_time", 0)))

    return [(f"{k}.id", order_info_id),
            (f"{k}.status", order_info_status),
            (f"{k}.payment", order_info_payment),
            (f"{k}.time", order_info_time)]


TYPE_MAPPER = {
    "int": int_mapper,
    "string": string_mapper,
    "array_int": array_int_mapper,
    "array_string": array_string_mapper,
    "bool": bool_mapper,
    "timestamp": timestamp_mapper,
    "tag_score_stats": tag_score_stats_mapper,
    "rule_stats": rule_stats_mapper,
    "emotion": type_count_mapper,
    "ab_normal": type_count_mapper,
    "excellent": type_count_mapper,
    "qc_word": qc_word_mapper,
    "order_info": order_info_mapper,
}


# 拉取当天生成的记录
def get_filter_dic(execution_date):
    # mongo date utc ,execution_data +8时区
    execution_date += timedelta(hours=-8)
    return {
        "begin_time": {
            "$gte": execution_date,
            "$lt": (execution_date + timedelta(days=1))
        }
    }


# 获取当天更新的记录(不包括今当天创建的记录)
def get_update_filter_dic(execution_date):
    # mongo date utc ,execution_data +8时区
    execution_date += timedelta(hours=-8)
    return {
        "update_time": {
            "$gte": execution_date,
            "$lt": (execution_date + timedelta(days=1))
        },
        "begin_time": {
            "$lt": execution_date
        }
    }


# 如果 ch_insert_day 不为空，则添加这个字段到每行记录，改字段用于插入更新表
def generate_iterator(docs, ch_insert_day):
    res_arr = []
    counter = 0
    if isinstance(docs, dict):
        yield [process(docs, ch_insert_day)]
    else:
        for doc in docs:
            res_arr.append(process(doc, ch_insert_day))
            counter += 1
            if counter == CHUNK_SIZE:
                log.info("Prepared")
                yield res_arr
                res_arr = []
                counter = 0
    if res_arr:
        log.info("Prepared")
        yield res_arr


def process(doc, ch_insert_day):
    tmp_arr = []

    for field_name in FIELDS:
        field_type = FIELDS[field_name]
        field_value = doc.get(field_name)
        kvs = TYPE_MAPPER[field_type](field_name, field_value)

        for kv in kvs:
            tmp_arr.append(kv)
    tmp_dict = dict(tmp_arr)
    if ch_insert_day:
        tmp_dict['ch_insert_day'] = ch_insert_day
    return tmp_dict


# 拉取当天生成的记录
def do_transfer(**kwargs):
    # day = datetime.strptime(kwargs["day"] + " 21:00:00", "%Y-%m-%d %H:%M:%S")
    day = datetime.strptime(kwargs["day"], "%Y-%m-%d")
    log.info(f"Start transfer data of [{day}]")

    # 拉取当天生成的记录
    mongo_client = MongoClient(MONGO_URL)
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]
    mongo_docs = collection.find(get_filter_dic(day))

    ch_client = Client(host=CH_HOST, port=CH_PORT)
    ds_nodash = int(kwargs["ds_nodash"])
    insert_sql = """ insert into buffer.xdqc_dialog_buffer values"""
    # 删除当天生成的记录(幂等)
    ch_client.execute(f"alter table ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r drop partition {ds_nodash}")
    log.info(f"alter table ods.xdqc_dialog_local ON CLUSTER cluster_3s_2r drop partition {ds_nodash} succerss")
    log.info("Preparing...")

    # 插入当天生成的记录
    for data in generate_iterator(mongo_docs, None):
        n = len(data)
        log.info(f"Transferring {n}...")
        ch_client.execute(insert_sql, data)
        log.info(f"Transferred {n}")
        log.info("Preparing...")
    log.info(f"Transfer data of [{day}] successfully")
    ch_client.disconnect()
    mongo_client.close()

    # 写入buffer表，需要等待一段时间，然后记录才会落盘
    time.sleep(60)
    # 去重
    optimize_table('ods.xdqc_dialog_local', ds_nodash)


# 获取当天更新的记录(不包括当天创建的记录)
def do_update_transfer(**kwargs):
    # day = datetime.strptime(kwargs["day"] + " 21:00:00", "%Y-%m-%d %H:%M:%S")
    day = datetime.strptime(kwargs["day"], "%Y-%m-%d")
    ds_nodash = int(kwargs["ds_nodash"])
    log.info(f"Start transfer data of [{day}]")

    # 获取当天更新的记录(不包括当天创建的记录)
    mongo_client = MongoClient(MONGO_URL)
    collection = mongo_client[MONGO_DB][MONGO_COLLECTION]
    mongo_docs = collection.find(get_update_filter_dic(day))

    # 删除已存的当天更新的过去记录(幂等)
    ch_client = Client(host=CH_HOST, port=CH_PORT)
    ch_client.execute(f"ALTER TABLE  ods.xdqc_dialog_update_local ON CLUSTER cluster_3s_2r drop partition {ds_nodash}")

    # 插入当天更新的过去记录
    insert_update_sql = """ insert into buffer.xdqc_dialog_update_buffer values"""
    log.info("Preparing...")
    for data in generate_iterator(mongo_docs, ds_nodash):
        n = len(data)
        log.info(f"Transferring {n}...")
        ch_client.execute(insert_update_sql, data)
        log.info(f"Transferred {n}")
        log.info("Preparing...")
    log.info(f"Transfer data of [{day}] successfully")
    ch_client.disconnect()
    mongo_client.close()

    # 写入buffer表，需要等待一段时间，然后记录才会落盘
    time.sleep(60)

    # 主动去重
    optimize_table('ods.xdqc_dialog_update_local', ds_nodash)


def optimize_table(table_name, partition_id):
    if not partition_id:
        print('partition id is not defined, return')
        return 0
    # time.sleep(15)
    ch = ClickHouseHook('clickhouse_zjk_008')
    try:
        optimize_sql = f"""optimize table {table_name} ON cluster cluster_3s_2r partition {partition_id} final"""
        print(optimize_sql)
        res = ch.execute(optimize_sql)
        print(str(res))
        time.sleep(10)
    except Exception as e:
        print(e)
        print("retry for once ,sleep 15s ")
        # time.sleep(15)
        optimize_sql = f"""optimize table {table_name} ON cluster cluster_3s_2r partition {partition_id}"""
        print(optimize_sql)
        res = ch.execute(optimize_sql)
        print(str(res))
        time.sleep(10)


# 拉取当天创建的记录 mongo -> ods.xdqc_dialog_all
do_transfer_job = PythonOperator(
    task_id="do_transfer_job",
    python_callable=do_transfer,
    op_kwargs={
        "day": "{{ execution_date.strftime(\"%Y-%m-%d\") }}",
        "ds_nodash": "{{ ds_nodash }}"
    },
    dag=dag
)

# 获取当天更新的记录(不包括当天创建的记录) mongo -> ods.xdqc_dialog_update_all
do_update_transfer_job = PythonOperator(
    task_id="do_update_transfer_job",
    python_callable=do_update_transfer,
    op_kwargs={
        "day": "{{ execution_date.strftime(\"%Y-%m-%d\") }}",
        "ds_nodash": "{{ ds_nodash }}"
    },
    dag=dag
)


# 写入当天创建的记录
def insert_t1_dialog_data(ds_nodash):
    cur_dt = datetime.strptime(ds_nodash, '%Y%m%d')
    ch = ClickHouseHook('clickhouse_zjk_008')
    start_time = cur_dt.strftime('%Y-%m-%d 00:00:00')
    end_time = cur_dt.strftime('%Y-%m-%d 24:00:00')

    sql = f"""insert into dwd.xdqc_dialog_all
select 
            _id ,platform ,channel ,`group` ,
            `date` ,seller_nick ,cnick ,snick ,begin_time ,
            end_time ,is_after_sale ,is_inside ,employee_name , s_emotion.type ,s_emotion.count ,c_emotion.type ,c_emotion.count ,emotions,
            abnormals.type ,abnormals.count ,excellents.type ,excellents.count ,    qc_word.source ,
            qc_word.word,qc_word.count ,qid,mark ,mark_judge ,mark_score ,mark_score_add ,mark_ids,last_mark_id ,
            human_check ,   tag_score_stats.id,tag_score_stats.score,tag_score_add_stats.id,tag_score_add_stats.score,rule_stats.id,
            rule_stats.score,rule_stats.count ,rule_add_stats.id,rule_add_stats.score,rule_add_stats.count ,score ,
            score_add , question_count , answer_count ,first_answer_time ,qa_time_sum ,
            qa_round_sum ,focus_goods_id ,is_remind ,task_list_id ,read_mark,   last_msg_id ,
            consulte_transfor_v2 ,order_info.id,order_info.status,
            order_info.payment,order_info.time,intel_score ,remind_ntype ,
            first_follow_up_time ,is_follow_up_remind ,emotion_detect_mode ,has_withdraw_robot_msg ,is_order_matched ,
            suspected_positive_emotion ,suspected_problem ,suspected_excellent ,has_after , cnick_customize_rule,
        update_time, 1 
from ods.xdqc_dialog_all where `begin_time`>=toDateTime64('{start_time}',3) and `begin_time`<=toDateTime64('{end_time}',3)
"""
    res = ch.execute(sql)
    print(res)


# 删除当天写入T+1质检记录(幂等性)
def clean_qc_dialog_one_local_partition(ds_nodash):
    ch = ClickHouseHook('clickhouse_zjk_008')
    ds_nodash = int(ds_nodash)
    ch_sql = f"alter table dwd.xdqc_dialog_local on cluster cluster_3s_2r drop partition {ds_nodash}"
    res = ch.execute(ch_sql)
    print(ch_sql)
    print(res)


def update_data_by_day(ds_nodash):
    ch = ClickHouseHook('clickhouse_zjk_008')
    cur_nodash = int(ds_nodash)
    # 前推30天记录
    cur_dt = datetime.strptime(ds_nodash, '%Y%m%d')

    # 当天创建的记录已经处理不需要重复更新，所以从1开始(即当天的前一天开始)
    for i in range(1, 30):
        job_dt = cur_dt + timedelta(days=-i)
        start_time = job_dt.strftime('%Y-%m-%d 00:00:00')
        end_time = job_dt.strftime('%Y-%m-%d 24:00:00')
        job_ds_nodash = int(job_dt.strftime('%Y%m%d'))
        if job_ds_nodash <= 20201111: # 设置最早日期
            break

        #  1、删除当天更新的，并且过去三十天内创建的记录(幂等)
        delete_ch_sql = f"""
insert into dwd.xdqc_dialog_all 
select 
    a._id ,platform ,channel ,`group` ,
    `date` ,seller_nick ,cnick ,snick ,begin_time ,
    end_time ,is_after_sale ,is_inside ,employee_name , s_emotion_type ,s_emotion_count ,c_emotion_type ,c_emotion_count ,emotions,
    abnormals_type ,abnormals_count ,excellents_type ,excellents_count ,qc_word_source ,
    qc_word_word,qc_word_count ,qid,mark ,mark_judge ,mark_score ,mark_score_add ,mark_ids,last_mark_id ,
    human_check, tag_score_stats_id, tag_score_stats_score, tag_score_add_stats_id,tag_score_add_stats_score,rule_stats_id,
    rule_stats_score,rule_stats_count ,rule_add_stats_id,rule_add_stats_score,rule_add_stats_count ,score ,
    score_add , question_count , answer_count ,first_answer_time ,qa_time_sum ,
    qa_round_sum ,focus_goods_id ,is_remind ,task_list_id ,read_mark, last_msg_id ,
    consulte_transfor_v2 ,order_info_id, order_info_status,
    order_info_payment, order_info_time, intel_score ,remind_ntype ,
    first_follow_up_time, is_follow_up_remind, emotion_detect_mode, has_withdraw_robot_msg, is_order_matched ,
    suspected_positive_emotion, suspected_problem, suspected_excellent, has_after, cnick_customize_rule,
    update_time, 
    -1
from (
    -- 获取过去某天创建的记录
    select * 
    from dwd.xdqc_dialog_all 
    where `begin_time`>=toDateTime64('{start_time}',3) and `begin_time`<=toDateTime64('{end_time}',3)  
) a
left join -- 既然使用left join保证左表记录非空,又使用where过滤右表记录非空,为何不直接inner join呢???
(   
    -- 获取当天更新的记录
    select  _id 
    from ods.xdqc_dialog_update_all 
    where ch_insert_day={cur_nodash}
) as b 
on a._id=b._id 
where b._id<>'' -- 仅保留右表非空的记录
        """
        print(delete_ch_sql)
        res = ch.execute(delete_ch_sql)
        print(str(res))
        time.sleep(15)
        # 2、刷新并合并记录
        try:
            optimize_sql = f"""optimize table dwd.xdqc_dialog_local ON cluster cluster_3s_2r partition {job_ds_nodash} final"""
            print(optimize_sql)
            res = ch.execute(optimize_sql)
            print(str(res))
            time.sleep(15)
        except Exception as e:
            print(e)
            print("retry for once ,sleep 15s ")
            time.sleep(15)
            optimize_sql = f"""optimize table dwd.xdqc_dialog_local ON cluster cluster_3s_2r partition {job_ds_nodash}"""
            print(optimize_sql)
            res = ch.execute(optimize_sql)
            print(str(res))
            time.sleep(15)

    # 3、写入新的更新记录
    last_30days_dt = (cur_dt + timedelta(days=-30))
    if last_30days_dt < datetime(2020, 11, 12): # 边界处理
        last_30days_dt = datetime(2020, 11, 12)
    last_30days = last_30days_dt.strftime('%Y-%m-%d 00:00:00')
    insert_data_sql = f"""
insert into dwd.xdqc_dialog_all
select 
        _id ,platform ,channel ,`group` ,
        `date` ,seller_nick ,cnick ,snick ,begin_time ,
        end_time ,is_after_sale ,is_inside ,employee_name , s_emotion.type ,s_emotion.count ,c_emotion.type ,c_emotion.count ,emotions,
        abnormals.type ,abnormals.count ,excellents.type ,excellents.count ,    qc_word.source ,
        qc_word.word,qc_word.count ,qid,mark ,mark_judge ,mark_score ,mark_score_add ,mark_ids,last_mark_id ,
        human_check ,   tag_score_stats.id,tag_score_stats.score,tag_score_add_stats.id,tag_score_add_stats.score,rule_stats.id,
        rule_stats.score,rule_stats.count ,rule_add_stats.id,rule_add_stats.score,rule_add_stats.count ,score ,
        score_add , question_count , answer_count ,first_answer_time ,qa_time_sum ,
        qa_round_sum ,focus_goods_id ,is_remind ,task_list_id ,read_mark,   last_msg_id ,
        consulte_transfor_v2, order_info.id, order_info.status,
        order_info.payment,order_info.time,intel_score ,remind_ntype ,
        first_follow_up_time ,is_follow_up_remind ,emotion_detect_mode ,has_withdraw_robot_msg ,is_order_matched ,
        suspected_positive_emotion ,suspected_problem ,suspected_excellent ,has_after , cnick_customize_rule,
    update_time, 1 
from ods.xdqc_dialog_update_all
where ch_insert_day={cur_nodash}  -- 查询当天更新的记录
and begin_time>=toDateTime64('{last_30days}',3) -- 查询过去三十天内创建的记录
    """
    print(insert_data_sql)
    res = ch.execute(insert_data_sql)
    print(str(res))
    time.sleep(2)


# 删除之前写入的当天创建的记录(幂等性) clear dwd.xdqc_dialog_all
clean_qc_dialog_one_local_partition_job = PythonOperator(
    task_id="clean_qc_dialog_one_local_partition_job",
    python_callable=clean_qc_dialog_one_local_partition,
    op_kwargs={
        "ds_nodash": "{{ ds_nodash }}"
    },
    dag=dag
)

# 写入当天创建的记录 ods.xdqc_dialog_all -> dwd.xdqc_dialog_all
insert_t1_dialog_data_job = PythonOperator(
    task_id="insert_t1_dialog_data_job",
    python_callable=insert_t1_dialog_data,
    op_kwargs={
        "ds_nodash": "{{ ds_nodash }}"
    },
    dag=dag
)

# 遍历当天更新的记录(不包括当天创建的记录),对过去三十天内创建的记录进行更新
update_data_by_day_job = PythonOperator(
    task_id="update_data_by_day_job",
    python_callable=update_data_by_day,
    op_kwargs={
        "ds_nodash": "{{ ds_nodash }}"
    },
    dag=dag
)

do_transfer_job >> do_update_transfer_job >> \
clean_qc_dialog_one_local_partition_job >> insert_t1_dialog_data_job >> \
update_data_by_day_job
