#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
import random
import multiprocessing


def func(x, y):
    ret = x / y
    return ret


def task(i):
    ts = random.randint(1, 10)
    time.sleep(ts)
    nums = [
        -1,
        3,
        1,
        2
    ]
    x, y = random.choice(nums), random.choice(nums)
    value = func(x, y)
    print(f'{i}执行完毕！耗时{ts}s,结果为{value}')
    return value


def error_callback(error):
    print(f"Error info: {error}")


if __name__ == '__main__':
    pool = multiprocessing.Pool(6)
    res = []
    for i in range(6):
        print(f"开始执行第{i}个任务...")
        result = pool.apply_async(task, args=(
            i,
        ))
        print(i, result)
        res.append(result)

    # output = [item.get() for item in res]
    # print(output)

    pool.close()
    pool.join()

    output = [item.get() for item in res]
    print(output)
