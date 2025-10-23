#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from concurrent import futures
import grpc
import logging
import time
import redis

from content_pb2_grpc import add_RecoServiceServicer_to_server, RecoServiceServicer
from content_pb2 import RecoRequest, RecoResponse


recall_content_type_list = [
    "album",
    "broadcast",
    "column",
    "single",
]

recall_reason_list = [
    "item2vec",
    "user_count",
    "all_content",
]

content_type_id_dict = {
    "album": 1,
    "broadcast": 2,
    "column": 3,
    "single": 7,
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
            start
        ]))
        print("request.device_uuid:", request.device_uuid, device_uuid)
        print("request.channel_id:", request.channel_id, channel_id)
        print("request.request_num:", request.request_num, request_num)
        print("log_key:", log_key)
        print()
        # print("type(context):", type(context))
        # print("context:", context)
        # print()

        redis_key_dict = {}
        for content_type_label in recall_content_type_list:
            redis_key_dict.setdefault(content_type_label, [])
            for recall_reason_label in recall_reason_list:
                if recall_reason_label == "item2vec":
                    tmp_key = device_uuid + "_" + channel_id + "_" + content_type_label + "_" + recall_reason_label
                else:
                    tmp_key = channel_id + "_" + content_type_label + "_" + recall_reason_label
                redis_key_dict[content_type_label].append(tmp_key)
                print(log_key, tmp_key)

        recall_from_redis = {}
        with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
            pipeline = client.pipeline()
            for content_type_label in recall_content_type_list:
                pipeline.mget(redis_key_dict[content_type_label])
                result = pipeline.execute()
                recall_from_redis[content_type_label] = result[0]
                print(log_key, "Result: ", content_type_label, " reco_from_redis: ", len(redis_key_dict[content_type_label]), recall_from_redis.keys())
        print()

        content_reco_dict = {}
        for content_type_label in recall_content_type_list:
            item2vec_count = 0
            user_count = 0
            all_content = 0
            tmp_list = []
            for recall_reason, content_id_str in zip(recall_reason_list, recall_from_redis[content_type_label]):
                if not content_id_str:
                    continue
                if recall_reason == "item2vec":
                    for item in content_id_str.decode("utf-8").split(";"):
                        if item in tmp_list:
                            continue
                        tmp_key = item + "|" + content_type_label + "|" + recall_reason + "|" + str(content_type_id_dict[content_type_label])
                        content_reco_dict.setdefault(content_type_label, [])
                        content_reco_dict[content_type_label].append(tmp_key)
                        tmp_list.append(item)
                        item2vec_count += 1
                        if item2vec_count >= 10:
                            break
                elif recall_reason == "user_count":
                    for item in content_id_str.decode("utf-8").split(";"):
                        if item in tmp_list:
                            continue
                        tmp_key = item + "|" + content_type_label + "|" + recall_reason + "|" + str(content_type_id_dict[content_type_label])
                        content_reco_dict.setdefault(content_type_label, [])
                        content_reco_dict[content_type_label].append(tmp_key)
                        tmp_list.append(item)
                        user_count += 1
                        if user_count >= 10:
                            break
                elif recall_reason == "all_content":
                    for item in content_id_str.decode("utf-8").split(";"):
                        if item in tmp_list:
                            continue
                        tmp_key = item + "|" + content_type_label + "|" + recall_reason + "|" + str(content_type_id_dict[content_type_label])
                        content_reco_dict.setdefault(content_type_label, [])
                        content_reco_dict[content_type_label].append(tmp_key)
                        tmp_list.append(item)
                        all_content += 1
                        # print(item2vec_count, user_count, all_content, item2vec_count + user_count + all_content)
                        if (item2vec_count + user_count + all_content) >= request_num:
                            break
            print(log_key, content_type_label, "item2vec_count, user_count, all_content, request_num: ", item2vec_count, user_count, all_content, request.request_num)

        reco_response = RecoResponse()
        content_id_list = []
        for key, value in content_reco_dict.items():
            for content_id in value:
                response = reco_response.content.add()
                response.content_id = content_id
                response.content_type = content_type_id_dict[key]
                content_id_list.append(content_id)

        print("reco_response:", reco_response)

        # for idx in range(len(content_id_list)):
        #     print(log_key, idx, content_id_list[idx])
        # print()

        print(log_key, "len(content_reco_dict)", len(content_reco_dict))
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
    logging.basicConfig()
    serve()
