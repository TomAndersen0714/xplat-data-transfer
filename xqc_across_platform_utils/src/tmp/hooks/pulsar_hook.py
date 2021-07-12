import hashlib
import json
import os
import time

from math import ceil
from pulsar import Client


class PulsarSendHook():
    def __init__(self, conf):
        self.service_url = conf["service_url"]
        self.database_url = conf["database_url"]
        self.worekr_name = conf["worekr_name"]
        self.done_files_dir = conf["done_files_dir"] + f'/{self.worekr_name}'
        self.output_dir = conf["output_dir"] + f'/{self.worekr_name}'
        self.pulsar_url = conf["pulsar_url"]
        self.topic = conf["topic"]
        self.export_cmd = conf["export_cmd"]
        self.payload_length = 1024 * 1024
        self.mkdir_work(self.done_files_dir)
        self.mkdir_work(self.output_dir)
        self.export_data()

    def mkdir_work(self, path, **kwargs):

        isExists = os.path.exists(path)
        # 判断结果
        if not isExists:
            # 如果不存在则创建目录
            # 创建目录操作函数
            os.makedirs(path)
            print(path + ' 创建成功')
            return True
        else:
            # 如果目录存在则不创建，并提示目录已存在
            print(path + ' 目录已存在')
            return False

    def export_data(self):
        cmd = f'{self.export_cmd} > {self.done_files_dir}/data.parq'
        print(cmd)
        try:
            # 导出parq文件
            os.system(cmd)
            # 切分parq文件
            # df = pd.read_parquet(self.done_files_dir, chunksize="100MB")
            # df = df.repartition(partition_size="100MB")
            # df.to_parquet(self.output_dir, write_index=False)
        except Exception as e:
            print(e)

    def get_done_files(self):
        done_file = []
        for parent, dirnames, filenames in os.walk(self.output_dir):
            for filename in filenames:
                file_path = os.path.join(parent, filename)  # 得到文件的绝对/相对路径
                if os.path.splitext(file_path)[1] == '.parquet':
                    done_file.append({"file": file_path, "length": os.path.getsize(file_path)})

                    """
                    #with open(cur_done_file(self.ds_nodash), 'r') as f:
                        return [l.strip('\n') for l in f.readlines()]
                    return []
                    """
        return done_file

    def pulsar_send(self, payload, total_parts, cur_part, ds_nodash, sync_id):

        headers = {
            "worekr_name": f"{self.worekr_name}",
            "partition": {"day": int(ds_nodash)},
            "sync_id": f"{sync_id}",
            "total_parts": total_parts,
            "cur_part": cur_part,
            "md5_checksum": hashlib.md5(payload).hexdigest(),
            "is_overwrite": False
        }
        print(json.dumps(headers, indent=2))
        data_header = json.dumps(headers).encode(encoding='utf8')

        # rep = requests.post(SERVICE_URL, data=data_header + b'\n' + payload, headers={"K8s-Apigate": "1"})
        cli = Client(self.pulsar_url)
        producer = cli.create_producer(self.topic)
        producer.send(data_header + b'\n' + payload)
        # print(rep.json())

    def do_file(self, f, ds_nodash):
        file_path = f["file"]  # 文件路径
        length = f["length"]  # 文件大小
        offset = 0
        total_parts = ceil(length / self.payload_length)
        sync_id = hashlib.md5(file_path.encode('utf8')).hexdigest()
        for i in range(0, total_parts):
            print(f"File {file_path} {i + 1} / {total_parts} ...")
            with open(file_path, 'rb') as f:
                f.seek(offset)
                payload = f.read(self.payload_length)
                offset += self.payload_length
            self.pulsar_send(payload, total_parts, i + 1, ds_nodash, sync_id)
            time.sleep(2)