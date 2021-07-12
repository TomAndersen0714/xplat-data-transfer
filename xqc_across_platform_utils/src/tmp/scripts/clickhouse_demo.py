#!/usr/bin/python3
from itertools import islice

from clickhouse_driver.dbapi.connection import Connection
from clickhouse_driver.client import Client


def dbapi_v1_demo():
    """
    Fetch data using the dbapi 1.0
    """
    # configurations
    ch_host = 'cdh2'
    ch_tcp_port = '29000'
    ch_select_sql = 'SELECT * FROM tmp.truncate_test_all'
    settings = {'max_block_size': 100000}

    # connect
    ch_client = Client(host=ch_host, port=ch_tcp_port)

    # execute and fetch all rows
    print(ch_client.execute(query=ch_select_sql))
    print()
    # execute and fetch all rows with column names and types
    # the first element is the list of tuple which consist of column name and corresponding type
    print(ch_client.execute(query=ch_select_sql, with_column_types=True))
    print()
    # execute and return a corresponding generator
    gen = ch_client.execute_iter(query=ch_select_sql, settings=settings, with_column_types=True)
    for content in gen:
        print(content)

    # 当使用迭代器获取数据时,max_block_size参数会控制clickhous-driver每次缓存的结果行数,并且当with_column_types=True
    # 时迭代的第一个元素为列和对应的类型,剩下的元素则为查询结果

    # 当直接获取全部执行结果时,则列信息和数据类型会作为list的最后一个元素

    # data insert test
    print()
    # ch_client.execute('truncate table tmp.truncate_test_local on cluster cluster_3s_2r ')
    # ch_client.execute('INSERT INTO tmp.truncate_test_all (a,b) VALUES', [{'a': 3, 'b': 'Tom'}])
    # ch_client.execute('INSERT INTO tmp.truncate_test_all (a,b) VALUES', [(3,'Tom')])
    # ch_client.execute('INSERT INTO tmp.truncate_test_all VALUES', [(7, 'Tom')])
    # ch_client.execute('INSERT INTO tmp.truncate_test_all VALUES', [[0, '0'], [1, '1']])


def dbapi_v2_demo():
    """
    Fetch data using the dbapi 2.0
    """

    # configurations
    ch_host = 'cdh2'
    ch_tcp_port = '29000'
    ch_select_sql = 'SELECT * FROM tmp.truncate_test_all'

    # connect
    # there is a bug that user and password must be set
    ch_conn = Connection(
        user='default', password='',
        host=ch_host, port=ch_tcp_port, database='default'
    )

    # create and config cursor
    cursor = ch_conn.cursor()
    # set the cursor to get the result batch-by-batch
    cursor.set_stream_results(stream_results=True, max_row_buffer=100000)
    # execute sql and cache the generator of cached data block
    cursor.execute(operation=ch_select_sql)
    # get the result in list(if stream_results=True) or generator(if stream_results=False)
    rows_list = cursor.fetchmany(size=3)
    print(rows_list)

    # close
    ch_conn.close()


if __name__ == '__main__':
    dbapi_v1_demo()
