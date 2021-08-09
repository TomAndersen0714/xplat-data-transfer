#!/usr/bin/python3

import pickle
import ast
import time
import datetime


def eval_vs_pickle(n):
    obj = [(1, 'Tom', (1, 2)), (2, 'Alice', (3, 4))]
    tmp_str = str(obj)
    tmp_bytes = pickle.dumps(obj)

    clock = time.time()
    # clock = datetime.datetime.now()
    for _ in range(n):
        # eval(tmp_str)
        ast.literal_eval(tmp_str)
    # print("'eval' deserialization cast: %ds" % (time.time() - clock))
    print("'ast.literal_eval' deserialization cast: %ds" % (time.time() - clock))
    # print("'eval' deserialization cast: %ds" % (datetime.datetime.now() - clock).seconds)

    clock = time.time()
    # clock = datetime.datetime.now()
    for _ in range(n):
        pickle.loads(tmp_bytes)
        pickle.loads(tmp_bytes)
    print("'pickle' deserialization cast: %ds" % (time.time() - clock))
    # print("'pickle' deserialization cast: %ds" % (datetime.datetime.now() - clock).seconds)


if __name__ == '__main__':
    eval_vs_pickle(1000000)
