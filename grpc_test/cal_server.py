#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from concurrent import futures
import grpc
import SimpleCal_pb2
import SimpleCal_pb2_grpc

class CalServicer(SimpleCal_pb2_grpc.CalServicer):
    def Add(self, request, context):
        print("Add function called", request.number1, request.number2)
        return SimpleCal_pb2.ResultReply(number=request.number1 + request.number2)

    def Multiply(self, request, context):
        print("Multiply service called", request.number1, request.number2)
        return SimpleCal_pb2.ResultReply(number=request.number1 * request.number2)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    SimpleCal_pb2_grpc.add_CalServicer_to_server(CalServicer(), server)
    server.add_insecure_port("10.129.23.11:4243")
    server.start()
    print("grpc server start...", "10.129.23.11:4243")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()








