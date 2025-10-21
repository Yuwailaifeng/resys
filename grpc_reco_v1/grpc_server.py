#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from concurrent import futures
import grpc
import logging
import time

from content_pb2_grpc import add_RecoServiceServicer_to_server, RecoServiceServicer
from content_pb2 import RecoRequest, RecoResponse


class Reco(RecoServiceServicer):
    def RecoSys(self, request, context):
        start = time.time()
        print("request.device_uuid:", request.device_uuid)
        print("request.channel_id:", request.channel_id)
        print("request.request_num:", request.request_num)
        print()
        print("type(context):", type(context))
        print("context:", context)
        print()

        reco_response = RecoResponse()
        for i in range(request.request_num):
            response = reco_response.content.add()
            response.content_id = i + 1
            response.content_type = 1

        content_id_list = [(i + 1) for i in range(request.request_num)]

        print("reco_response:", reco_response)
        print("content_id_list", content_id_list)
        print()
        print("Total time %s" % (time.time() - start))

        # return reco_response
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
