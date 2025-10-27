#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import os
import sys
import redis
import datetime
import pandas as pd
import numpy as np

print(sys.version_info)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

file_hour = (datetime.datetime.now() + datetime.timedelta(days=-1)).date().strftime("%Y%m%d")
print("file_hour", file_hour)

recall_name_list = [
    "album",
    # "broadcast",
    # "column",
    "single",
]

all_content_recall_file_list = [
    "item2vec_1/model_data/" + str(file_hour) + ".content_album_for_recs.txt",
    # "item2vec_2/model_data/" + str(file_hour) + ".content_broadcast_for_recs.txt",
    # "item2vec_3/model_data/" + str(file_hour) + ".content_column_for_recs.txt",
    "item2vec_7/model_data/" + str(file_hour) + ".content_single_for_recs.txt",
]

user_count_recall_file_list = [
    "item2vec_1/model_data/" + str(file_hour) + ".content_count.txt",
    # "item2vec_2/model_data/" + str(file_hour) + ".content_count.txt",
    # "item2vec_3/model_data/" + str(file_hour) + ".content_count.txt",
    "item2vec_7/model_data/" + str(file_hour) + ".content_count.txt",
]

item2vec_recall_file_list = [
    "item2vec_1/model_data/" + str(file_hour) + ".user_content_reco.txt",
    # "item2vec_2/model_data/" + str(file_hour) + ".user_content_reco.txt",
    # "item2vec_3/model_data/" + str(file_hour) + ".user_content_reco.txt",
    "item2vec_7/model_data/" + str(file_hour) + ".user_content_reco.txt",
]

# print("item2vec_recall_file_list", item2vec_recall_file_list)

for file_name in all_content_recall_file_list:
    os.system("wc -l " + file_name)

for file_name in user_count_recall_file_list:
    os.system("wc -l " + file_name)

for file_name in item2vec_recall_file_list:
    os.system("wc -l " + file_name)

batch_size = 10000
top_k = 1000
key_num = 0

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("user_count start", start_time)

user_count_reco_dict = {}
for recall_name, file_name in zip(recall_name_list, user_count_recall_file_list):
    with open(file_name, encoding="UTF-8") as file:
        for line in file.readlines():
            try:
                line = line.strip().split("\t")
                if len(line) != 3:
                    print(line)
                    continue
                content_id = line[0]
                channel_id_list = line[2].split("|")[-1].split("#")
                for channel_id in channel_id_list:
                    user_count_reco_dict.setdefault(channel_id + "_" + recall_name + "_user_count", [])
                    user_count_reco_dict[channel_id + "_" + recall_name + "_user_count"].append(content_id)
            except:
                print(line)
            # if count >= 100:
            #     break

print("len(user_count_reco_dict)", len(user_count_reco_dict))

for key, value in user_count_reco_dict.items():
    user_count_reco_dict[key] = ";".join(value[:top_k])
print("len(user_count_reco_dict)", len(user_count_reco_dict))
key_num += len(user_count_reco_dict)

with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
    print("redis_version: ", client.info()["redis_version"])
    client.flushdb()
    pipeline = client.pipeline()
    pipeline.mset(user_count_reco_dict)
    result = pipeline.execute()
    print("Result: user_count_reco_dict: ", len(user_count_reco_dict), ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("user_count done", done_time)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("all_content start", start_time)

all_content_reco_dict = {}
for recall_name, file_name in zip(recall_name_list, all_content_recall_file_list):
    with open(file_name, encoding="UTF-8") as file:
        for line in file.readlines():
            try:
                line = line.strip().split("\t")
                if len(line) < 3 or "id" in line[0]:
                    print(line)
                    continue
                content_id, channel_id = line[0], line[-1]
                all_content_reco_dict.setdefault(channel_id + "_" + recall_name + "_all_content", [])
                all_content_reco_dict[channel_id + "_" + recall_name + "_all_content"].append(content_id)
            except:
                print(line)
            # if count >= 100:
            #     break

print("len(all_content_reco_dict)", len(all_content_reco_dict))

for key, value in all_content_reco_dict.items():
    all_content_reco_dict[key] = ";".join(value[:top_k])
print("len(all_content_reco_dict)", len(all_content_reco_dict))
key_num += len(all_content_reco_dict)

with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
    # print("redis_version: ", client.info()["redis_version"])
    # client.flushdb()
    pipeline = client.pipeline()
    pipeline.mset(all_content_reco_dict)
    result = pipeline.execute()
    print("Result: all_content_reco_dict: ", len(all_content_reco_dict), ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("all_content done", done_time)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("album_item2vec start", start_time)

count = 0
album_reco_dict = {}
with open(item2vec_recall_file_list[0], encoding="UTF-8") as file:
    for line in file.readlines():
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            album_reco_dict.setdefault(count // batch_size, {})
            album_reco_dict[count // batch_size][line[0].split("|")[0] + "_album_item2vec"] = line[1]
            count += 1
        except:
            print(line)

print("len(album_reco_dict)", len(album_reco_dict), count)
key_num += count

with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
    # print("redis_version: ", client.info()["redis_version"])
    # client.flushdb()
    pipeline = client.pipeline()
    for key, value in album_reco_dict.items():
        # print(key, value)
        pipeline.mset(value)
        result = pipeline.execute()
        print("Result: album_reco_dict: ", key, ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("album_item2vec done", done_time)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# print("broadcast_item2vec start", start_time)
#
# count = 0
# broadcast_reco_dict = {}
# with open(item2vec_recall_file_list[1], encoding="UTF-8") as file:
#     for line in file.readlines():
#         try:
#             line = line.strip().split("\t")
#             if len(line) != 2:
#                 print(line)
#                 continue
#             device_uuid = line[0].split("|")[0]
#             reco_content_id_list = [item.split("|")[0] for item in line[1].split(";")]
#             broadcast_reco_dict.setdefault(count // batch_size, {})
#             broadcast_reco_dict[count // batch_size][device_uuid + "_broadcast_item2vec"] = ";".join(reco_content_id_list[:top_k])
#             count += 1
#         except:
#             print(line)
#
# print("len(broadcast_reco_dict)", len(broadcast_reco_dict), count)
# key_num += count
#
# with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
#     # print("redis_version: ", client.info()["redis_version"])
#     pipeline = client.pipeline()
#     for key, value in broadcast_reco_dict.items():
#         # print(key, value)
#         pipeline.mset(value)
#         result = pipeline.execute()
#         print("Result: broadcast_reco_dict: ", key, ": ", result)
#
# done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# print("broadcast_item2vec done", done_time)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# print("column_item2vec start", start_time)
#
# count = 0
# column_reco_dict = {}
# with open(item2vec_recall_file_list[2], encoding="UTF-8") as file:
#     for line in file.readlines():
#         try:
#             line = line.strip().split("\t")
#             if len(line) != 2:
#                 print(line)
#                 continue
#             device_uuid = line[0].split("|")[0]
#             reco_content_id_list = [item.split("|")[0] for item in line[1].split(";")]
#             column_reco_dict.setdefault(count // batch_size, {})
#             column_reco_dict[count // batch_size][device_uuid + "_column_item2vec"] = ";".join(reco_content_id_list[:top_k])
#             count += 1
#         except:
#             print(line)
#
# print("len(column_reco_dict)", len(column_reco_dict), count)
# key_num += count
#
# with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
#     # print("redis_version: ", client.info()["redis_version"])
#     pipeline = client.pipeline()
#     for key, value in column_reco_dict.items():
#         # print(key, value)
#         pipeline.mset(value)
#         result = pipeline.execute()
#         print("Result: column_reco_dict: key: ", key, ": ", result)
#
# done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# print("column_item2vec done", done_time)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("single_item2vec start", start_time)

count = 0
single_reco_dict = {}
with open(item2vec_recall_file_list[3], encoding="UTF-8") as file:
    for line in file.readlines():
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            single_reco_dict.setdefault(count // batch_size, {})
            single_reco_dict[count // batch_size][line[0].split("|")[0] + "_single_item2vec"] = line[1]
            count += 1
        except:
            print(line)

print("len(single_reco_dict)", len(single_reco_dict), count)
key_num += count

with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
    # print("redis_version: ", client.info()["redis_version"])
    pipeline = client.pipeline()
    for key, value in single_reco_dict.items():
        # print(key, value)
        pipeline.mset(value)
        result = pipeline.execute()
        print("Result: single_reco_dict: key: ", key, ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("single_item2vec done", done_time)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

print("len(all_content_reco_dict)", len(all_content_reco_dict))
print("len(user_count_reco_dict)", len(user_count_reco_dict))
print("len(album_reco_dict)", len(album_reco_dict))
# print("len(broadcast_reco_dict)", len(broadcast_reco_dict))
# print("len(column_reco_dict)", len(column_reco_dict))
print("len(single_reco_dict)", len(single_reco_dict))
print("key_num", key_num)

print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print(str(file_hour) + " DONE!")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
