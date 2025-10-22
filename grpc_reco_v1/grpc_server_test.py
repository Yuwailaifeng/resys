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


class Reco(RecoServiceServicer):
    def RecoSys(self, request, context):
        start = time.time()
        print("request.device_uuid:", request.device_uuid)
        print("request.channel_id:", request.channel_id)
        print("request.request_num:", request.request_num)
        print()
        # print("type(context):", type(context))
        # print("context:", context)
        # print()

        recall_redis_key = []
        for content_type_label in recall_content_type_list:
            for recall_reason_label in recall_reason_list:
                if recall_reason_label == "item2vec":
                    tmp_key = request.device_uuid + "_" + request.channel_id + "_" + content_type_label + "_" + recall_reason_label
                else:
                    tmp_key = request.channel_id + "_" + content_type_label + "_" + recall_reason_label
                recall_redis_key.append(tmp_key)
                print(tmp_key)

        with redis.Redis(host="10.129.23.11", port=6379, db=0) as client:
            pipeline = client.pipeline()
            pipeline.mget(recall_redis_key)
            result = pipeline.execute()
            print("Result: user_count_reco_dict: ", len(recall_redis_key), ": ", len(result[0]))
            print()

        recall_res = result[0]
        reco_response = RecoResponse()
        content_id_list = []
        content_type_id = 0
        for idx in range(len(recall_res)):
            if not recall_res[idx]:
                continue
            if idx // len(recall_reason_list) == 0:
                content_type_id = 1
            elif idx // len(recall_reason_list) == 1:
                content_type_id = 2
            elif idx // len(recall_reason_list) == 2:
                content_type_id = 3
            elif idx // len(recall_reason_list) == 3:
                content_type_id = 7

            tmp_recall_list = recall_res[idx].decode("utf-8").split(";")
            for i in range(request.request_num):
                if i >= len(tmp_recall_list):
                    break
                response = reco_response.content.add()
                response.content_id = tmp_recall_list[i] + "|" + str(idx) + "|" + str(recall_content_type_list[idx // len(recall_reason_list)]) + "|" + str(recall_reason_list[idx % len(recall_reason_list)]) + "|" + str(content_type_id)
                response.content_type = content_type_id
                content_id_list.append(tmp_recall_list[i] + "|" + str(idx) + "|" + str(recall_content_type_list[idx // len(recall_reason_list)]) + "|" + str(recall_reason_list[idx % len(recall_reason_list)]) + "|" + str(content_type_id))

        print("reco_response:", reco_response)

        for idx in range(len(content_id_list)):
            print(idx, content_id_list[idx])
        print()

        print("len(content_id_list)", len(content_id_list))
        print("Total time %s" % (time.time() - start))

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
