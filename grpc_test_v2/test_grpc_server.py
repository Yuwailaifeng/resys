#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from concurrent import futures
import grpc
import logging
import time

from video_pb2_grpc import add_RecoServiceServicer_to_server, \
    RecoServiceServicer
from video_pb2 import RecoRequest, RecoReply


class Reco(RecoServiceServicer):
    # 这里实现我们定义的接口
    def SayReco(self, request, context):
        print(time)
        print(time)
        print(time)
        score_list = [i+i/10 for i in range(10)]
        print("type(request.video): ", type(request.video))
        print("request.video[1]: ", request.video[1])
        print("request.video: ", request.video)
        print()
        print("type(request): ", type(request))
        print("request: ", request)
        print()
        print("type(context): ", type(context))
        print("context: ", context)
        print()
        return RecoReply(message="Reco, {}! ".format(request.video), score_list=score_list)


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

