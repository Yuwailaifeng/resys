#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import logging

import grpc
from video_pb2 import RecoRequest, RecoReply
from video_pb2_grpc import RecoServiceStub

def run():
    # 使用with语法保证channel自动close
    with grpc.insecure_channel("10.129.23.11:4245") as channel:
        # 客户端通过stub来实现rpc通信
        stub = RecoServiceStub(channel)
        print("grpc server send...", "10.129.23.11:4245")

        # 客户端必须使用定义好的类型，这里是RecoRequest类型
        recorequest = RecoRequest()
        for i in range(5):
            request = recorequest.video.add()
            request.age = i + 10
            request.name = "Title" + "_" + str(i)

        print("recorequest: ", recorequest)
        response = stub.SayReco(recorequest)
    print("response: ", response)
    print("reco client received: ", response.message)
    print("response.score_list", response.score_list)

if __name__ == "__main__":
    logging.basicConfig()
    run()


