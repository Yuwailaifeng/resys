#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# import zerorpc

# c = zerorpc.Client()

# c.connect("tcp://127.0.0.1:4242")

# print(c.add(1, 2))
# print(c.power(2, 7))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# import zerorpc
# import time

# def zerorpc_client():
#     print("zerorpc client")
#     c = zerorpc.Client()
#     print("CONNECT: ", "tcp://127.0.0.1:4243")
#     c.connect("tcp://127.0.0.1:4243")
#     data = "RPC Client"
#     start = time.time()
#     for i in range(500):
#         a = c.getObj()
#         print(a)
#     for i in range(500):
#         c.sendObj(data)

#     print("total time %s" % (time.time() - start))


# if __name__ == "__main__":
#     zerorpc_client()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import zerorpc
import time

# 创建RPC客户端
client = zerorpc.Client()

# print("CONNECT: ", "tcp://127.0.0.1:4242")
# client.connect("tcp://127.0.0.1:4242")

print("CONNECT: ", "tcp://10.129.23.11:4244")
client.connect("tcp://10.129.23.11:4244")

data = {}
for i in range(10):
    data[str(i)] = i

try:
    # 调用远程函数
    response = client.hello("RPC", timeout=1)
    print(response)
    response = client.process_data(data, timeout=1)
    print(response)
    start = time.time()
    for i in range(10):
        response = client.process_data(data, timeout=10)
    print("Total time %s" % (time.time() - start))
except zerorpc.exceptions.LostRemote as e:
    print("远程连接断开：", e)
except zerorpc.exceptions.RemoteError as e:
    print("远程服务器错误：", e)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# import sys
# sys.set_int_max_str_digits(0)

# import json
# import requests

# #自己实现1个rpc框架
# class ClientStub():
#     def __init__(self, url):
#         self.url = url

#     def add(self, arg1, arg2):
#         remote_call_result = requests.get(url="%s?arg1=%s&arg2=%s" % (self.url, arg1, arg2))
#         remote_call_result = json.loads(remote_call_result.text).get("result", 0)
#         return remote_call_result

# # http的调用
# # client = ClientStub(url="http://127.0.0.1:4244/")
# client = ClientStub(url="http://10.211.55.2:4244/")

# print(client.add(2, 3))
# print(client.add(2, 5))
# print(client.add(2, 10000))













