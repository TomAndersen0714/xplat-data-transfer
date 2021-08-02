import uuid

batch_id = str(uuid.uuid4())
header: dict[str:str] = {
    'task_id': 'test',
    "db_type": "clickhouse",
    "target_table": 'tmp.drop_partition_test_all',
    "clear_table": 'tmp.drop_partition_test_local',
    "partition": "(2021,7,13)",
    "cluster_name": "cluster_3s_2r",
    "batch_id": batch_id
}
