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
    "single",
]

all_content_recall_file_list = [
    "item2vec_1/model_data/" + str(file_hour) + ".content_album_for_recs.txt",
    "item2vec_7/model_data/" + str(file_hour) + ".content_single_for_recs.txt",
]

user_count_recall_file_list = [
    "item2vec_1/model_data/" + str(file_hour) + ".content_count.txt",
    "item2vec_7/model_data/" + str(file_hour) + ".content_count.txt",
]

user_trigger_file_list = [
    "item2vec_1/sample_data/" + str(file_hour) + ".device_uuid_content_id_sequence.txt",
    "item2vec_7/sample_data/" + str(file_hour) + ".device_uuid_content_id_sequence.txt",
]

i2i_recall_file_list = [
    "item2vec_1/model_data/" + str(file_hour) + ".content_similarity_channel.txt",
    "item2vec_7/model_data/" + str(file_hour) + ".content_similarity_channel.txt",
]

u2i_recall_file_list = [
    "item2vec_1/model_data/" + str(file_hour) + ".user_content_reco_channel.txt",
    "item2vec_7/model_data/" + str(file_hour) + ".user_content_reco_channel.txt",
]

for file_name in all_content_recall_file_list:
    os.system("wc -l " + file_name)

for file_name in user_count_recall_file_list:
    os.system("wc -l " + file_name)

for file_name in user_trigger_file_list:
    os.system("wc -l " + file_name)

for file_name in i2i_recall_file_list:
    os.system("wc -l " + file_name)

for file_name in u2i_recall_file_list:
    os.system("wc -l " + file_name)

batch_size = 10000
top_k = 1000
key_num = 0

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

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

with redis.Redis(host="10.129.23.11", port=6379, db=2) as client:
    print("redis_version: ", client.info()["redis_version"])
    client.flushdb()
    pipeline = client.pipeline()
    pipeline.mset(user_count_reco_dict)
    result = pipeline.execute()
    print("Result: user_count_reco_dict: ", len(user_count_reco_dict), ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("user_count done", key_num, done_time, "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

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

with redis.Redis(host="10.129.23.11", port=6379, db=2) as client:
    # print("redis_version: ", client.info()["redis_version"])
    # client.flushdb()
    pipeline = client.pipeline()
    pipeline.mset(all_content_reco_dict)
    result = pipeline.execute()
    print("Result: all_content_reco_dict: ", len(all_content_reco_dict), ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("all_content done", key_num, done_time, "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("album_i2i_trigger start", start_time)

count = 0
album_i2i_trigger_dict = {}
with open(user_trigger_file_list[0], encoding="UTF-8") as file:
    for line in file.readlines():
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            album_i2i_trigger_dict[line[0] + "_album_i2i_trigger"] = line[1]
            count += 1
        except:
            print(line)

print("len(album_i2i_trigger_dict)", len(album_i2i_trigger_dict), count)
key_num += count

with redis.Redis(host="10.129.23.11", port=6379, db=2) as client:
    # print("redis_version: ", client.info()["redis_version"])
    # client.flushdb()
    pipeline = client.pipeline()
    pipeline.mset(album_i2i_trigger_dict)
    result = pipeline.execute()
    print("Result: album_i2i_trigger_dict: ", len(album_i2i_trigger_dict), ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("album_i2i_trigger done", key_num, done_time, "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("single_i2i_trigger start", start_time)

count = 0
single_i2i_trigger_dict = {}
with open(user_trigger_file_list[1], encoding="UTF-8") as file:
    for line in file.readlines():
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            single_i2i_trigger_dict[line[0] + "_single_i2i_trigger"] = line[1]
            count += 1
        except:
            print(line)

print("len(single_i2i_trigger_dict)", len(single_i2i_trigger_dict), count)
key_num += count

with redis.Redis(host="10.129.23.11", port=6379, db=2) as client:
    # print("redis_version: ", client.info()["redis_version"])
    # client.flushdb()
    pipeline = client.pipeline()
    pipeline.mset(single_i2i_trigger_dict)
    result = pipeline.execute()
    print("Result: single_i2i_trigger_dict: ", len(single_i2i_trigger_dict), ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("single_i2i_trigger done", key_num, done_time, "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("album_i2i start", start_time)

count = 0
album_i2i_dict = {}
with open(i2i_recall_file_list[0], encoding="UTF-8") as file:
    for line in file.readlines():
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            album_i2i_dict[line[0].split("|")[0] + "_album_i2i"] = line[1]
            count += 1
        except:
            print(line)

print("len(album_i2i_dict)", len(album_i2i_dict), count)
key_num += count

with redis.Redis(host="10.129.23.11", port=6379, db=2) as client:
    # print("redis_version: ", client.info()["redis_version"])
    # client.flushdb()
    pipeline = client.pipeline()
    pipeline.mset(album_i2i_dict)
    result = pipeline.execute()
    print("Result: album_i2i_dict: ", len(album_i2i_dict), ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("album_i2i done", key_num, done_time, "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("single_i2i start", start_time)

count = 0
single_i2i_dict = {}
with open(i2i_recall_file_list[1], encoding="UTF-8") as file:
    for line in file.readlines():
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            single_i2i_dict[line[0].split("|")[0] + "_single_i2i"] = line[1]
            count += 1
        except:
            print(line)

print("len(single_i2i_dict)", len(single_i2i_dict), count)
key_num += count

with redis.Redis(host="10.129.23.11", port=6379, db=2) as client:
    # print("redis_version: ", client.info()["redis_version"])
    # client.flushdb()
    pipeline = client.pipeline()
    pipeline.mset(single_i2i_dict)
    result = pipeline.execute()
    print("Result: single_i2i_dict: ", len(single_i2i_dict), ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("single_i2i done", key_num, done_time, "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("album_u2i start", start_time)

count = 0
album_u2i_dict = {}
with open(u2i_recall_file_list[0], encoding="UTF-8") as file:
    for line in file.readlines():
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            album_u2i_dict.setdefault(count // batch_size, {})
            album_u2i_dict[count // batch_size][line[0].split("|")[0] + "_album_u2i"] = line[1]
            count += 1
        except:
            print(line)

print("len(album_u2i_dict)", len(album_u2i_dict), count)
key_num += count

with redis.Redis(host="10.129.23.11", port=6379, db=2) as client:
    # print("redis_version: ", client.info()["redis_version"])
    # client.flushdb()
    pipeline = client.pipeline()
    for key, value in album_u2i_dict.items():
        # print(key, value)
        pipeline.mset(value)
        result = pipeline.execute()
        print("Result: album_u2i_dict: ", key, ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("album_u2i done", key_num, done_time, "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("single_u2i start", start_time)

count = 0
single_u2i_dict = {}
with open(u2i_recall_file_list[1], encoding="UTF-8") as file:
    for line in file.readlines():
        try:
            line = line.strip().split("\t")
            if len(line) != 2:
                print(line)
                continue
            single_u2i_dict.setdefault(count // batch_size, {})
            single_u2i_dict[count // batch_size][line[0].split("|")[0] + "_single_u2i"] = line[1]
            count += 1
        except:
            print(line)

print("len(single_u2i_dict)", len(single_u2i_dict), count)
key_num += count

with redis.Redis(host="10.129.23.11", port=6379, db=2) as client:
    # print("redis_version: ", client.info()["redis_version"])
    # client.flushdb()
    pipeline = client.pipeline()
    for key, value in single_u2i_dict.items():
        # print(key, value)
        pipeline.mset(value)
        result = pipeline.execute()
        print("Result: single_u2i_dict: key: ", key, ": ", result)

done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("single_u2i done", key_num, done_time, "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

print("len(all_content_reco_dict)", len(all_content_reco_dict))
print("len(user_count_reco_dict)", len(user_count_reco_dict))
print("len(album_i2i_trigger_dict)", len(album_i2i_trigger_dict))
print("len(single_i2i_trigger_dict)", len(single_i2i_trigger_dict))
print("len(album_i2i_dict)", len(album_i2i_dict))
print("len(single_i2i_dict)", len(single_i2i_dict))
print("len(album_u2i_dict)", len(album_u2i_dict))
print("len(single_u2i_dict)", len(single_u2i_dict))
print("key_num", key_num)

print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), key_num, " content2redis DONE!")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
