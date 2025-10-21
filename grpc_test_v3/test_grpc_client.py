#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
import grpc
from video_pb2 import RecoRequest, RecoResponse
from video_pb2_grpc import RecoServiceStub


def run():
    start = time.time()
    # 使用with语法保证channel自动close
    with grpc.insecure_channel("10.129.23.11:4245") as channel:
        # with grpc.insecure_channel("127.0.0.1:4245") as channel:
        # 客户端通过stub来实现rpc通信
        stub = RecoServiceStub(channel)
        print("grpc server send...", "10.129.23.11:4245")
        # 客户端必须使用定义好的类型，这里是RecoRequest类型

        reco_request = RecoRequest(user_id=1, channel_id="时政", network="WiFi", request_num=3)
        print("type(reco_request.user_id):", type(reco_request.user_id))
        print("type(reco_request.channel_id):", type(reco_request.channel_id))
        print("type(reco_request.network):", type(reco_request.network))
        print("type(reco_request.request_num):", type(reco_request.request_num))
        print("reco_request.user_id:", reco_request.user_id)
        print("reco_request.channel_id:", reco_request.channel_id)
        print("reco_request.network:", reco_request.network)
        print("reco_request.request_num:", reco_request.request_num)

        response = stub.RecoSys(reco_request)
        print("type(response): ", type(response))
        print("type(response.video): ", type(response.video))
        print("type(response.video_id_list): ", type(response.video_id_list))
        print("response: ", response)
        print("response.video: ", response.video)
        print("response.video_id_list: ", response.video_id_list)
    print("Total time %s" % (time.time() - start))


if __name__ == "__main__":
    run()
