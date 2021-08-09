import uuid

batch_id = str(uuid.uuid4())
header1: dict[str:str] = {
    'task_id': 'test',
    "db_type": "kudu",
    "write_mode": 'upsert',  # [upsert, insert]
    "source_table": 'tmp.kudu_test_table',
    "target_table": 'tmp.kudu_test_table',
    "batch_id": batch_id
}
