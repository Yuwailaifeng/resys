#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from concurrent import futures
import grpc
import time
import datetime
import redis
import random

from content_pb2_grpc import add_RecoServiceServicer_to_server, RecoServiceServicer
from content_pb2 import RecoRequest, RecoResponse


recall_content_type_list = [
    "album",
    "single",
]

recall_reason_list = [
    "i2i_trigger",
    "user_count",
    "all_content",
]

content_type_id_dict = {
    "album": 1,
    "single": 3,
}


# ALBUM(1, "专辑"),
# COLUMN(2,"栏目"),
# SINGLE(3,"单曲"),
# COLUMN_SINGLE(4,"栏目单曲"),
# RADIO(5,"广播"),
# LIVE_ROOM_B(185, "B类直播间"),
# NEWS(23, "资讯"),
# ADVERTISING(150, "广告"),
# TAG_REC(151, "标签簇"),


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


class Reco(RecoServiceServicer):
    def RecoSys(self, request, context):
        start = time.time()
        device_uuid = request.device_uuid
        channel_id = request.channel_id
        request_num = request.request_num
        log_key = "|".join(map(str, [
            device_uuid,
            channel_id,
            request_num,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ]))
        print("request.device_uuid:", request.device_uuid, device_uuid)
        print("request.channel_id:", request.channel_id, channel_id)
        print("request.request_num:", request.request_num, request_num)
        print("log_key:", log_key)
        print()
        # print("type(context):", type(context))
        # print("context:", context)
        # print()

        redis_key_list = []
        for content_type_label in recall_content_type_list:
            for recall_reason_label in recall_reason_list:
                if recall_reason_label == "i2i_trigger":
                    tmp_key = device_uuid + "_" + content_type_label + "_" + recall_reason_label
                else:
                    tmp_key = channel_id + "_" + content_type_label + "_" + recall_reason_label
                redis_key_list.append(tmp_key)
                print(log_key, tmp_key, len(redis_key_list))

        with redis.Redis(host="10.129.23.11", port=6379, db=1) as client:
            pipeline = client.pipeline()
            pipeline.mget(redis_key_list)
            result = pipeline.execute()
            album_i2i_trigger = result[0][0].decode("utf-8").split(";") if result[0][0] else []
            album_user_count = result[0][1].decode("utf-8").split(";") if result[0][1] else []
            album_all_content = result[0][2].decode("utf-8").split(";") if result[0][2] else []
            single_i2i_trigger = result[0][3].decode("utf-8").split(";") if result[0][3] else []
            single_user_count = result[0][4].decode("utf-8").split(";") if result[0][4] else []
            single_all_content = result[0][5].decode("utf-8").split(";") if result[0][5] else []
            print(log_key, "Result: ", "reco_from_redis: ", len(result[0]))
            print(log_key, "len(album_i2i_trigger): ", len(album_i2i_trigger))
            print(log_key, "len(album_user_count): ", len(album_user_count))
            print(log_key, "len(album_all_content): ", len(album_all_content))
            print(log_key, "len(single_i2i_trigger): ", len(single_i2i_trigger))
            print(log_key, "len(single_user_count): ", len(single_user_count))
            print(log_key, "len(single_all_content): ", len(single_all_content))
        print()

        album_i2i_reco_list = []
        if len(album_i2i_trigger) > 0:
            album_i2i_key_list = [item + "_" + channel_id + "_album_i2i" for item in album_i2i_trigger]
            with redis.Redis(host="10.129.23.11", port=6379, db=1) as client:
                pipeline = client.pipeline()
                pipeline.mget(album_i2i_key_list)
                result = pipeline.execute()
                album_i2i_reco_list = [item.decode("utf-8").split(";") if item else [] for item in result[0]]
                print(log_key, "Result: ", "album_i2i_reco_from_redis: ", len(result[0]))
                print(log_key, "len(album_i2i_trigger): ", len(album_i2i_trigger))
                print(log_key, "len(album_i2i_key_list): ", len(album_i2i_key_list))
                print(log_key, "len(album_i2i_reco_list): ", len(album_i2i_reco_list))
                print("album_i2i_trigger", album_i2i_trigger)
                print("album_i2i_key_list", album_i2i_key_list)
                for i in range(len(album_i2i_trigger)):
                    print(i, "album_i2i_key_list", album_i2i_trigger[i], album_i2i_reco_list[i][:10])
        print()

        single_i2i_reco_list = []
        if len(single_i2i_trigger) > 0:
            single_i2i_key_list = [item + "_" + channel_id + "_single_i2i" for item in single_i2i_trigger]
            with redis.Redis(host="10.129.23.11", port=6379, db=1) as client:
                pipeline = client.pipeline()
                pipeline.mget(single_i2i_key_list)
                result = pipeline.execute()
                single_i2i_reco_list = [item.decode("utf-8").split(";") if item else [] for item in result[0]]
                print(log_key, "Result: ", "single_i2i_reco_from_redis: ", len(result[0]))
                print(log_key, "len(single_i2i_trigger): ", len(single_i2i_trigger))
                print(log_key, "len(single_i2i_key_list): ", len(single_i2i_key_list))
                print(log_key, "len(single_i2i_reco_list): ", len(single_i2i_reco_list))
                print(log_key, "len(single_i2i_reco_list[0]): ", len(single_i2i_reco_list[0]))
                print("single_i2i_trigger", single_i2i_trigger)
                print("single_i2i_key_list", single_i2i_key_list)
                for i in range(len(single_i2i_trigger)):
                    print(i, "single_i2i_reco_list", single_i2i_trigger[i], single_i2i_reco_list[i][:10])
            print()

        content_reco_result = []
        content_reco_set = set()
        num = len(content_reco_result)
        if len(album_i2i_reco_list) > 0:
            for idx in range(10):
                if len(content_reco_result) - num >= request_num:
                    break
                for contend_id_list in album_i2i_reco_list:
                    if len(contend_id_list) > idx and contend_id_list[idx] not in content_reco_set and contend_id_list[idx] not in album_i2i_trigger:
                        content_reco_result.append(contend_id_list[idx] + "|" + str(content_type_id_dict["album"]) + "|i2i")
                        content_reco_set.add(contend_id_list[idx])
        album_i2i_reco_num = len(content_reco_result) - num
        print("album_i2i_reco_list ", album_i2i_reco_num, "len(content_reco_result)", len(content_reco_result))

        num = len(content_reco_result)
        tmp_list = album_user_count[:10]
        random.shuffle(tmp_list)
        tmp_list.extend(album_user_count[10:])
        album_user_count = tmp_list
        for contend_id in album_user_count:
            if len(content_reco_result) - num >= request_num:
                break
            if contend_id not in content_reco_set and contend_id not in album_i2i_trigger:
                content_reco_result.append(contend_id + "|" + str(content_type_id_dict["album"]) + "|user_count")
                content_reco_set.add(contend_id)
        album_user_count_reco_num = len(content_reco_result) - num
        print("album_user_count ", album_user_count_reco_num, "len(content_reco_result)", len(content_reco_result))

        num = len(content_reco_result)
        random.shuffle(album_all_content)
        for contend_id in album_all_content:
            if len(content_reco_result) - num >= request_num:
                break
            if contend_id not in content_reco_set and contend_id not in album_i2i_trigger:
                content_reco_result.append(contend_id + "|" + str(content_type_id_dict["album"]) + "|all_content")
                content_reco_set.add(contend_id)
        album_all_content_reco_num = len(content_reco_result) - num
        print("album_all_content ", album_all_content_reco_num, "len(content_reco_result)", len(content_reco_result))

        num = len(content_reco_result)
        if len(single_i2i_reco_list) > 0:
            for idx in range(10):
                if len(content_reco_result) - num >= request_num:
                    break
                for contend_id_list in single_i2i_reco_list:
                    if len(contend_id_list) > idx and contend_id_list[idx] not in content_reco_set and contend_id_list[idx] not in single_i2i_trigger:
                        content_reco_result.append(contend_id_list[idx] + "|" + str(content_type_id_dict["single"]) + "|i2i")
                        content_reco_set.add(contend_id_list[idx])
        single_i2i_reco_num = len(content_reco_result) - num
        print("single_i2i_reco_list ", single_i2i_reco_num, "len(content_reco_result)", len(content_reco_result))

        num = len(content_reco_result)
        tmp_list = single_user_count[:10]
        random.shuffle(tmp_list)
        tmp_list.extend(single_user_count[10:])
        single_user_count = tmp_list
        for contend_id in single_user_count:
            if len(content_reco_result) - num >= request_num:
                break
            if contend_id not in content_reco_set and contend_id not in single_i2i_trigger:
                content_reco_result.append(contend_id + "|" + str(content_type_id_dict["single"]) + "|user_count")
                content_reco_set.add(contend_id)
        single_user_count_reco_num = len(content_reco_result) - num
        print("single_user_count ", single_user_count_reco_num, "len(content_reco_result)", len(content_reco_result))

        num = len(content_reco_result)
        random.shuffle(single_all_content)
        for contend_id in single_all_content:
            if len(content_reco_result) - num >= request_num:
                break
            if contend_id not in content_reco_set and contend_id not in single_i2i_trigger:
                content_reco_result.append(contend_id + "|" + str(content_type_id_dict["single"]) + "|all_content")
                content_reco_set.add(contend_id)
        single_all_content_reco_num = len(content_reco_result) - num
        print("single_all_content ", single_all_content_reco_num, "len(content_reco_result)", len(content_reco_result))

        # u2i_redis_key_list = []
        # for content_type_label in recall_content_type_list:
        #     tmp_key = device_uuid + "_" + channel_id + "_" + content_type_label + "_u2i"
        #     u2i_redis_key_list.append(tmp_key)
        #     print(log_key, tmp_key, len(u2i_redis_key_list), u2i_redis_key_list)
        #
        # with redis.Redis(host="10.129.23.11", port=6379, db=1) as client:
        #     pipeline = client.pipeline()
        #     pipeline.mget(u2i_redis_key_list)
        #     result = pipeline.execute()
        #     album_u2i_reco_list = result[0][0].decode("utf-8").split(";") if result[0][0] else []
        #     single_u2i_reco_list = result[0][1].decode("utf-8").split(";") if result[0][1] else []
        #     print(log_key, "Result: ", "u2i_reco_from_redis: ", len(result[0]))
        #     print("result[0]", result[0][0])
        #     print(log_key, "len(album_u2i_reco_list): ", len(album_u2i_reco_list))
        #     print(log_key, "len(single_u2i_reco_list): ", len(single_u2i_reco_list))
        # print()
        #
        # num = len(content_reco_result)
        # for contend_id in album_u2i_reco_list:
        #     if len(content_reco_result) - num >= 10:
        #         break
        #     content_reco_result.append(contend_id + "|" + str(content_type_id_dict["album"]) + "|u2i")
        # print("album_u2i_reco_list ", "len(album_u2i_reco_list)", len(album_u2i_reco_list))
        #
        # num = len(content_reco_result)
        # for contend_id in single_u2i_reco_list:
        #     if len(content_reco_result) - num >= 10:
        #         break
        #     content_reco_result.append(contend_id + "|" + str(content_type_id_dict["single"]) + "|u2i")
        # print("single_u2i_reco_list ", "len(single_u2i_reco_list)", len(single_u2i_reco_list))

        reco_response = RecoResponse()
        content_id_list = []
        for item in content_reco_result:
            content_id, content_type, recall_reason = item.split("|")
            response = reco_response.content.add()
            response.content_id = content_id
            response.content_type = int(content_type)
            response.recall_reason = recall_reason
            content_id_list.append(item)

        # print("reco_response:", reco_response)

        for idx in range(len(content_id_list)):
            print(log_key, idx, content_id_list[idx])
        print()

        print(log_key, "album_i2i_reco_num", album_i2i_reco_num)
        print(log_key, "album_user_count_reco_num", album_user_count_reco_num)
        print(log_key, "album_all_content_reco_num", album_all_content_reco_num)
        print(log_key, "single_i2i_reco_num", single_i2i_reco_num)
        print(log_key, "single_user_count_reco_num", single_user_count_reco_num)
        print(log_key, "single_all_content_reco_num", single_all_content_reco_num)

        print(log_key, "len(content_reco_result)", len(content_reco_result))
        print(log_key, "len(content_id_list)", len(content_id_list))
        print(log_key, "Total time %s" % (time.time() - start))

        # ps aux | grep grpc_server | awk '{print $2}' | xargs kill -9
        # return RecoResponse(message="Reco, {}! ".format(reco_response.content), content_id_list=content_id_list)
        return RecoResponse(content=reco_response.content, content_id_list=content_id_list)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_RecoServiceServicer_to_server(Reco(), server)
    server.add_insecure_port("[::]:4245")
    server.start()
    print("grpc server start...", "[::]:4245")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
