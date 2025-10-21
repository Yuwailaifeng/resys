#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import os
import sys
from impala.dbapi import connect
import datetime
import pandas as pd
from collections import OrderedDict

from tensorflow.python.ops.nn_ops import top_k

print(sys.version_info)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

import heapq
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

file_hour = (datetime.datetime.now() + datetime.timedelta(days=-1)).date().strftime("%Y%m%d")
print("file_hour", file_hour)

dir_list = [
    "item2vec_1/model_data/" + str(file_hour) + ".vectors_similarity.txt",
    "item2vec_2/model_data/" + str(file_hour) + ".vectors_similarity.txt",
    "item2vec_3/model_data/" + str(file_hour) + ".vectors_similarity.txt",
    "item2vec_7/model_data/" + str(file_hour) + ".vectors_similarity.txt",
]
print("dir_list", dir_list)

batch_size = 100
top_k = 100

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

count = 0
album_reco_dict = {}
with open(dir_list[0], encoding="UTF-8") as file:
    for line in file.readlines():
        count += 1
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            content_id, channel_id_list = line[0].split("|")[0], line[0].split("|")[1].split("#")
            reco_content_id_list = [item.split("|")[0] for item in line[1].split(";")]
            for channel_id in channel_id_list:
                # album_reco_dict.setdefault(channel_id, {})
                reco_content_id_list = [item.split("|")[0] for item in line[1].split(";") if channel_id in item.split("|")[1].split("#")]
                # album_reco_dict[channel_id + "_" + content_id] = ";".join(reco_content_id_list[:10])
                album_reco_dict.setdefault(count // batch_size, {})
                album_reco_dict[count // batch_size][channel_id + "_" + content_id + "_album_i2i"] = ";".join(reco_content_id_list[:top_k])
        except:
            print(line)
        # if count >= 100:
        #     break

print("len(album_reco_dict)", len(album_reco_dict))

# for key, value in album_reco_dict.items():
#     print(key, value)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

count = 0
broadcast_reco_dict = {}
with open(dir_list[1], encoding="UTF-8") as file:
    for line in file.readlines():
        count += 1
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            content_id, channel_id_list = line[0].split("|")[0], line[0].split("|")[1].split("#")
            reco_content_id_list = [item.split("|")[0] for item in line[1].split(";")]
            for channel_id in channel_id_list:
                # broadcast_reco_dict.setdefault(channel_id, {})
                reco_content_id_list = [item.split("|")[0] for item in line[1].split(";") if channel_id in item.split("|")[1].split("#")]
                # broadcast_reco_dict[channel_id + "_" + content_id] = ";".join(reco_content_id_list[:10])
                broadcast_reco_dict.setdefault(count // batch_size, {})
                broadcast_reco_dict[count // batch_size][channel_id + "_" + content_id + "_broadcast_i2i"] = ";".join(reco_content_id_list[:top_k])
        except:
            print(line)
        # if count >= 100:
        #     break

print("len(broadcast_reco_dict)", len(broadcast_reco_dict))

# for key, value in broadcast_reco_dict.items():
#     print(key, value)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

count = 0
column_reco_dict = {}
with open(dir_list[2], encoding="UTF-8") as file:
    for line in file.readlines():
        count += 1
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            content_id, channel_id_list = line[0].split("|")[0], line[0].split("|")[1].split("#")
            reco_content_id_list = [item.split("|")[0] for item in line[1].split(";")]
            for channel_id in channel_id_list:
                # column_reco_dict.setdefault(channel_id, {})
                reco_content_id_list = [item.split("|")[0] for item in line[1].split(";") if channel_id in item.split("|")[1].split("#")]
                # column_reco_dict[channel_id + "_" + content_id] = ";".join(reco_content_id_list[:10])
                column_reco_dict.setdefault(count // batch_size, {})
                column_reco_dict[count // batch_size][channel_id + "_" + content_id + "_column_i2i"] = ";".join(reco_content_id_list[:top_k])
        except:
            print(line)
        # if count >= 100:
        #     break

print("len(column_reco_dict)", len(column_reco_dict))

# for key, value in column_reco_dict.items():
#     print(key, value)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

count = 0
single_reco_dict = {}
with open(dir_list[3], encoding="UTF-8") as file:
    for line in file.readlines():
        count += 1
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            content_id, channel_id_list = line[0].split("|")[0], line[0].split("|")[1].split("#")
            reco_content_id_list = [item.split("|")[0] for item in line[1].split(";")]
            for channel_id in channel_id_list:
                # single_reco_dict.setdefault(channel_id, {})
                reco_content_id_list = [item.split("|")[0] for item in line[1].split(";") if channel_id in item.split("|")[1].split("#")]
                # single_reco_dict[channel_id + "_" + content_id] = ";".join(reco_content_id_list[:10])
                single_reco_dict.setdefault(count // batch_size, {})
                single_reco_dict[count // batch_size][channel_id + "_" + content_id + "_single_i2i"] = ";".join(reco_content_id_list[:top_k])
        except:
            print(line)
        # if count >= 100:
        #     break

print("len(single_reco_dict)", len(single_reco_dict), "\n")

# for key, value in single_reco_dict.items():
#     print(key, value)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import redis

with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
    print("redis_version: ", client.info()["redis_version"])
    client.flushdb()
    pipeline = client.pipeline()

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    for key, value in album_reco_dict.items():
        # print(key, value)
        pipeline.mset(value)
        result = pipeline.execute()
        print("Result: album_reco_dict: ", key, ": ", result)
    print("len(album_reco_dict)", len(album_reco_dict), "\n")

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    for key, value in broadcast_reco_dict.items():
        # print(key, value)
        pipeline.mset(value)
        result = pipeline.execute()
        print("Result: broadcast_reco_dict: ", key, ": ", result)
    print("len(broadcast_reco_dict)", len(broadcast_reco_dict), "\n")

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    for key, value in column_reco_dict.items():
        # print(key, value)
        pipeline.mset(value)
        result = pipeline.execute()
        print("Result: column_reco_dict: key: ", key, ": ", result)
    print("len(column_reco_dict)", len(column_reco_dict), "\n")

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    for key, value in single_reco_dict.items():
        # print(key, value)
        pipeline.mset(value)
        result = pipeline.execute()
        print("Result: single_reco_dict: key: ", key, ": ", result)
    print("len(single_reco_dict)", len(single_reco_dict), "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# import redis
#
# client = redis.Redis(
#     host="10.129.23.11",
#     port=6379,
#     db=0,
# )
# print("redis_version: ", client.info()["redis_version"])
#
# pipeline = client.pipeline()
#
# print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
# for key, value in broadcast_reco_dict.items():
#     # print(key, value)
#     pipeline.mset(value)
#     result = pipeline.execute()
#     print("Result: album_reco_dict: ", key, ": ", result)
# print("len(album_reco_dict)", len(album_reco_dict), "\n")
#
# print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
# for key, value in broadcast_reco_dict.items():
#     # print(key, value)
#     pipeline.mset(value)
#     result = pipeline.execute()
#     print("Result: broadcast_reco_dict: ", key, ": ", result)
# print("len(broadcast_reco_dict)", len(broadcast_reco_dict), "\n")
#
# print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
# for key, value in column_reco_dict.items():
#     # print(key, value)
#     pipeline.mset(value)
#     result = pipeline.execute()
#     print("Result: column_reco_dict: key: ", key, ": ", result)
# print("len(column_reco_dict)", len(column_reco_dict), "\n")
#
# print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
# for key, value in single_reco_dict.items():
#     # print(key, value)
#     pipeline.mset(value)
#     result = pipeline.execute()
#     print("Result: single_reco_dict: key: ", key, ": ", result)
# print("len(single_reco_dict)", len(single_reco_dict), "\n")
#
# client.close()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print(str(file_hour) + " DONE!")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# import redis
#
# r = redis.Redis(host='10.129.23.11', port=6379, password='1234567890')
# with r.pipeline() as ctx:
#     a = time.time()
#     ctx.hset('current', "time2", a)
#     ctx.hset('current', "time3", a)
#     res = ctx.execute()
#     print("result: ", res)
#
# import redis
#
# r = redis.Redis(host='localhost', port=6379, db=0)
# data = {f'key{i}': f'value{i}' for i in range(100)}
# r.mset(data)
