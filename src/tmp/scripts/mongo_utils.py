#!/usr/bin/python3

import getopt
import sys

from pymongo import MongoClient
from typing import List, Tuple

mongo_default_server_host = "10.0.0.8"
mongo_default_server_port = 27017
mongo_default_server_user = "xdmp"
mongo_default_server_passwd = "20E6QK8V"

# mongo_default_server_user = "root"
# mongo_default_server_passwd = "3SqzSt65"

# jd 10.0.0.8:27017 xdmp 20E6QK8V
# ks 10.23.111.51:27017 root 3SqzSt65
# mini 10.22.131.22:27017 root 3SqzSt65


def parse_args_by_getopt(args: List[str]) -> Tuple[str, List[str]]:
    """Parse Parameters and return url for connection and other parameters."""
    host, port, user, passwd = "", "", "", ""
    try:
        opt_args, no_opt_args = getopt.getopt(args, "h:p:u:i:", ["host=", "port=", "user=", "password=", "help"])
    except getopt.GetoptError:
        print("usage: parse_args_demo.py [{-h|--host=}<host>] [{-p|--port=}<port>] "
              "[{-u|--user=}<user>] [{-i|--password=}<passwd>] [database [table [rows]]]")
        sys.exit(2)
    for opt, arg in opt_args:
        if opt in ("-h", "--host"):
            host = arg
        elif opt in ("-p", "--port"):
            port = arg
        elif opt in ("-u", "--user"):
            user = arg
        elif opt in ("-i", "--password"):
            passwd = arg
        elif opt == '--help':
            print("usage: parse_args_demo.py [{-h|--host=}<host>] [{-p|--port=}<port>] "
                  "[{-u|--user=}<user>] [{-i|--password=}<passwd>] [database [table [rows]]]")
            sys.exit(0)

    if not passwd: passwd = mongo_default_server_passwd
    if not user: user = mongo_default_server_user
    if not host: host = mongo_default_server_host
    if not port: port = mongo_default_server_port
    return f'mongodb://{user}:{passwd}@{host}:{port}', no_opt_args


def connect_and_find(uri: str, database: str = None, table: str = None, rows: int = 0):
    """
    Connect MongoDB Database and print first several rows of corresponding table.
    """
    mongo_client = MongoClient(uri)

    # 默认输出所有库名
    if not database :
        database_names = mongo_client.list_database_names()
        print(f"All databases:")
        for name in database_names:
            print(name)
    # 未指定表名时,输出所有表名
    elif not table :
        current_db = mongo_client[database]
        collection_names = current_db.list_collection_names()
        print(f"All collections in {database}:")
        for name in collection_names:
            print(name)
    # 未指定输出的前几行时,默认不查询
    elif rows == 0:
        current_db = mongo_client[database]
        current_collection = current_db[table]

        # PS: 此方法禁止使用,当数据量过大时即便是空过滤查询耗时也很长
        # filter_conditions_dict = {}
        # doc_count = current_collection.count_documents(filter_conditions_dict)

        # 查看表的行数和索引
        doc_count = current_collection.count()
        index_cursor = current_collection.list_indexes()
        print(f"The number of {database}.{table} documents:", doc_count)
        print(f"The indexes of {database}.{table}:")
        for index in index_cursor:
            print(index)
    else:
        current_db = mongo_client[database]
        current_collection = current_db[table]
        for doc in current_collection.find().limit(rows):
            print(doc)

    mongo_client.close()


if __name__ == '__main__':
    # demo
    # python3 mongo_utils.py -h 10.0.0.127 -p 30808 -u xdmp -i 20E6QK8V xd_stat stat_question_for_shop
    args = sys.argv[1:]
    uri, rest_args = parse_args_by_getopt(args)
    print(uri, rest_args)

    # limit the number of non-opt params
    while len(rest_args) < 3:
        rest_args.append('')
    while len(rest_args) > 3:
        rest_args.pop()

    if not rest_args[2] or int(rest_args[2]) < 0:
        rest_args[2] = '0'

    connect_and_find(uri, rest_args[0], rest_args[1], int(rest_args[2]))
