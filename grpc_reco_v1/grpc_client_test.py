#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
import grpc
import random
from content_pb2 import RecoRequest, RecoResponse
from content_pb2_grpc import RecoServiceStub


def run(idx, device_uuid, channel_id):
    start = time.time()
    with grpc.insecure_channel("10.129.23.11:4245") as channel:
        stub = RecoServiceStub(channel)
        print("grpc server send...", "10.129.23.11:4245")

        reco_request = RecoRequest(device_uuid=device_uuid, channel_id=str(channel_id), request_num=30)
        # reco_request = RecoRequest(device_uuid="82377867-D0AE-46DB-8A41-B05A11EBABAC", channel_id="1623515436593418240", request_num=30)
        # print("type(reco_request.device_uuid):", type(reco_request.device_uuid))
        # print("type(reco_request.channel_id):", type(reco_request.channel_id))
        # print("type(reco_request.request_num):", type(reco_request.request_num))
        # print("reco_request.device_uuid:", reco_request.device_uuid)
        # print("reco_request.channel_id:", reco_request.channel_id)
        # print("reco_request.request_num:", reco_request.request_num)
        # print()

        response = stub.RecoSys(reco_request)
        # print("type(response): ", type(response))
        # print("type(response.content): ", type(response.content))
        # print("type(response.content_id_list): ", type(response.content_id_list))
        # print("response: ", response)
        # print("response.content: ", response.content)
        # print("response.content_id_list: ", response.content_id_list)
        print("len(response.content_id_list): ", len(response.content_id_list))
    print(idx, device_uuid, channel_id, "Total time %s" % (time.time() - start), "\n")


if __name__ == "__main__":

    device_uuid_list = []
    # with open("../item2vec/impala_data/20251022.device_uuid_content_id.txt", encoding="UTF-8") as file:
    with open("../item2vec/sample_data/20251027.device_uuid_content_id_sequence.txt", encoding="UTF-8") as file:
        for line in file.readlines():
            tmp = line.strip().split("\t")
            device_uuid_list.append(tmp[0])
    print("len(device_uuid_list)", len(device_uuid_list))
    # device_uuid_list = device_uuid_list[:10]

    channel_id_list = [
        1624027415938576384,
        1623994492711493632,
        1625080144286928896,
        1620974685804187648,
        1623628664992366592,
        1625081590034161664,
        1625337162234990592,
        1623983072225660928,
        1623515436593418240,
        1625081767709007872,
        1623983440816939008,
        1858721047596187648,
        1625080080533475328,
        1623993297578160128,
    ]

    random.seed(1)
    for idx, device_uuid in enumerate(device_uuid_list):
        # channel_id = random.choices(channel_id_list, k=1)[0]
        channel_id = channel_id_list[random.randint(0, len(channel_id_list) - 1)]
        run(idx + 1, device_uuid, channel_id)
