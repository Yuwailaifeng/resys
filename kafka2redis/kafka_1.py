#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
import json
import redis
import datetime
from collections import OrderedDict
from kafka import KafkaConsumer

file_hour = (datetime.datetime.now() + datetime.timedelta(days=-1)).date().strftime("%Y%m%d")
print("file_hour", file_hour)

user_trigger_file_list = [
    "/data/azx_reco/yt_recall/item2vec_album/sample_data/" + str(file_hour) + ".device_uuid_content_id_sequence.txt",
    "/data/azx_reco/yt_recall/item2vec_single/sample_data/" + str(file_hour) + ".device_uuid_content_id_sequence.txt",
]


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# print("album_i2i_trigger start", start_time)
#
# count = 0
# key_num = 0
# album_i2i_trigger_dict = {}
# with open(user_trigger_file_list[0], encoding="UTF-8") as file:
#     for line in file.readlines():
#         try:
#             line = line.strip().split("\t")
#             if len(line) != 2:
#                 print(line)
#                 continue
#             album_i2i_trigger_dict[line[0] + "_album_i2i_trigger"] = line[1]
#             count += 1
#         except:
#             print(line)
#
# print("len(album_i2i_trigger_dict)", len(album_i2i_trigger_dict), count)
# key_num += count
#
# with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
#     print("redis_version: ", client.info()["redis_version"])
#     client.flushdb()
#     pipeline = client.pipeline()
#     pipeline.mset(album_i2i_trigger_dict)
#     result = pipeline.execute()
#     print("Result: album_i2i_trigger_dict: ", len(album_i2i_trigger_dict), ": ", result)
#
# done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# print("album_i2i_trigger done", key_num, done_time, "\n")
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# print("single_i2i_trigger start", start_time)
#
# count = 0
# single_i2i_trigger_dict = {}
# with open(user_trigger_file_list[1], encoding="UTF-8") as file:
#     for line in file.readlines():
#         try:
#             line = line.strip().split("\t")
#             if len(line) != 2:
#                 print(line)
#                 continue
#             single_i2i_trigger_dict[line[0] + "_single_i2i_trigger"] = line[1]
#             count += 1
#         except:
#             print(line)
#
# print("len(single_i2i_trigger_dict)", len(single_i2i_trigger_dict), count)
# key_num += count
#
# with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
#     # print("redis_version: ", client.info()["redis_version"])
#     # client.flushdb()
#     pipeline = client.pipeline()
#     pipeline.mset(single_i2i_trigger_dict)
#     result = pipeline.execute()
#     print("Result: single_i2i_trigger_dict: ", len(single_i2i_trigger_dict), ": ", result)
#
# done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# print("single_i2i_trigger done", key_num, done_time, "\n")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def item2redis(start_time, uuid, albumid, songid, timestampByNewADD):
    log_key = "|".join(map(str, [
        uuid,
        albumid,
        songid,
        timestampByNewADD,
    ]))

    redis_key_list = [
        uuid + "_album_i2i_trigger",
        uuid + "_single_i2i_trigger",
    ]

    with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
        # client.flushdb()
        pipeline = client.pipeline()
        pipeline.mget(redis_key_list)
        result = pipeline.execute()
        album_i2i_trigger = result[0][0].decode("utf-8").split(";") if result[0][0] else []
        single_i2i_trigger = result[0][1].decode("utf-8").split(";") if result[0][1] else []
        # print(log_key, "Result: ", "from_redis: ", len(result[0]))
        # print(log_key, "result from redis: len(album_i2i_trigger): ", len(album_i2i_trigger))
        # print(log_key, "result from redis: len(single_i2i_trigger): ", len(single_i2i_trigger))
        # print()

        content_dict = {}
        if albumid and len(albumid) > 0:
            album_i2i_trigger.insert(0, albumid)
            album_i2i_unique_list = list(OrderedDict.fromkeys(album_i2i_trigger))
            content_dict[redis_key_list[0]] = ";".join(album_i2i_unique_list)
            # if len(album_i2i_unique_list) > 1:
            #     print(log_key, redis_key_list[0], album_i2i_unique_list)

        if songid and len(songid) > 0:
            single_i2i_trigger.insert(0, songid)
            single_i2i_unique_list = list(OrderedDict.fromkeys(single_i2i_trigger))
            content_dict[redis_key_list[1]] = ";".join(single_i2i_unique_list)
            # if len(single_i2i_unique_list) > 1:
            #     print(log_key, redis_key_list[1], single_i2i_unique_list)

        pipeline.mset(content_dict)
        result = pipeline.execute()
        print(log_key, len(content_dict), "Result: ", result, time.time() - start_time)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

consumer = KafkaConsumer(
    "recommend_topic",
    group_id="reco_gpu_kafka_1",
    bootstrap_servers=[
        "10.129.31.22:9092",
        "10.129.31.23:9092",
        "10.129.31.24:9092",
    ],
    auto_offset_reset="earliest",
)

count_all = 0
count_redis = 0
while True:
    for msg in consumer:
        count_all += 1
        try:
            start_time = time.time()
            json_data = json.loads(msg.value.decode("utf-8"))
            if json_data["dsource"] == "albumv3":
                # print(json_data["uuid"], json_data["albumid"], json_data["songid"], json_data["timestampByNewADD"])
                item2redis(start_time, json_data["uuid"], json_data["albumid"], json_data["songid"], json_data["timestampByNewADD"])
                count_redis += 1
        except:
            # print("Except", msg)
            continue

        if count_all % 100000 == 0:
            print("count_redis", count_redis, count_all, count_redis / count_all)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
