#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import SimpleCal_pb2
import SimpleCal_pb2_grpc
import grpc

def run(a, b):
    channel = grpc.insecure_channel("10.129.23.11:4243") # 连接上gRPC服务端
    print("grpc server send...", "10.129.23.11:4243")
    stub = SimpleCal_pb2_grpc.CalStub(channel)

    response = stub.Add(SimpleCal_pb2.AddRequest(number1=a, number2=b))
    print(f"{a} + {b} = {response.number}")

    response = stub.Multiply(SimpleCal_pb2.MultiplyRequest(number1=a, number2=b))
    print(f"{a} * {b} = {response.number}")

if __name__ == "__main__":
    run(100, 200)





