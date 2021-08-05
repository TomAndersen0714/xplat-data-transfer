#!/usr/bin/python3
from impala.dbapi import connect

from impala.hiveserver2 import HiveServer2Connection


def hive_test():
    hs2_host, hs2_port = '10.0.2.2', 21050
    hs2_connect: HiveServer2Connection = connect(host=hs2_host, port=hs2_port)
    hs2_cursor = hs2_connect.cursor()
    row_batch_size = 100

    hs2_cursor.execute('SELECT * FROM tmp.kudu_test_table LIMIT 10')
    while True:
        batch = hs2_cursor.fetchmany(row_batch_size)
        print(type(batch), ': batch')
        if batch:
            for row in batch:
                print(type(row), row)
        else:
            break


if __name__ == '__main__':
    hive_test()
