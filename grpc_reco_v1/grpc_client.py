#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
import grpc
from content_pb2 import RecoRequest, RecoResponse
from content_pb2_grpc import RecoServiceStub


def run():
    start = time.time()
    # with grpc.insecure_channel("127.0.0.1:4245") as channel:
    with grpc.insecure_channel("10.129.23.11:4245") as channel:
        stub = RecoServiceStub(channel)
        print("grpc server send...", "10.129.23.11:4245")

        # reco_request = RecoRequest(device_uuid="0b7223a8-38d0-450f-8ce4-185e0b1ec9eb", channel_id="1623515436593418240", request_num=30)
        reco_request = RecoRequest(device_uuid="82377867-D0AE-46DB-8A41-B05A11EBABAC", channel_id="1623515436593418240", request_num=30)
        # print("type(reco_request.device_uuid):", type(reco_request.device_uuid))
        # print("type(reco_request.channel_id):", type(reco_request.channel_id))
        # print("type(reco_request.request_num):", type(reco_request.request_num))
        print("reco_request.device_uuid:", reco_request.device_uuid)
        print("reco_request.channel_id:", reco_request.channel_id)
        print("reco_request.request_num:", reco_request.request_num)
        print()

        response = stub.RecoSys(reco_request)
        # print("type(response): ", type(response))
        # print("type(response.content): ", type(response.content))
        # print("type(response.content_id_list): ", type(response.content_id_list))
        print("response: ", response)
        # print("response.content: ", response.content)
        # print("response.content_id_list: ", response.content_id_list)
        print("len(response.content_id_list): ", len(response.content_id_list))
    print("Total time %s" % (time.time() - start))


if __name__ == "__main__":
    run()
