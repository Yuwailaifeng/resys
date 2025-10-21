#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from concurrent import futures
import grpc
import logging
import time

from video_pb2_grpc import add_RecoServiceServicer_to_server, RecoServiceServicer
from video_pb2 import RecoRequest, RecoResponse


class Reco(RecoServiceServicer):
    # 这里实现我们定义的接口
    def RecoSys(self, request, context):
        start = time.time()
        print("request.user_id:", request.user_id)
        print("request.channel_id:", request.channel_id)
        print("request.network:", request.network)
        print("request.request_num:", request.request_num)
        print()
        print("type(context):", type(context))
        print("context:", context)
        print()

        reco_response = RecoResponse()
        for i in range(request.request_num):
            response = reco_response.video.add()
            response.video_id = i + 1
            response.title = "Title" + "_" + str(i + 1)
            response.score = i + 1 + (i + 1) / 10

        video_id_list = [(i + 1) for i in range(request.request_num)]

        print("reco_response:", reco_response)
        print("video_id_list", video_id_list)
        print()
        print("Total time %s" % (time.time() - start))

        # return reco_response
        # return RecoResponse(message="Reco, {}! ".format(reco_response.video), video_id_list=video_id_list)
        return RecoResponse(video=reco_response.video, video_id_list=video_id_list)


# message RecoResponse {
#     repeated Video video = 1;
# }

# message Video {
#     int32 video_id = 1;
#     string title = 2;
#     double score = 3;
# }


def serve():
    # 这里通过thread pool来并发处理server的任务
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # 将对应的任务处理函数添加到rpc server中
    add_RecoServiceServicer_to_server(Reco(), server)

    # 这里使用的非安全接口，世界gRPC支持TLS/SSL安全连接，以及各种鉴权机制
    server.add_insecure_port("[::]:4245")
    server.start()
    print("grpc server start...", "[::]:4245")
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
