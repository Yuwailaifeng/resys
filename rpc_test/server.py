#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# import zerorpc

# class caculate(object):
#     def hello(self, name):
#         return "hello, {}".format(name)

#     def add(self, x, y):
#         return x + y

#     def multiply(self, x, y):
#         return x * y

#     def subtract(self, x, y):
#         return abs(x - y)

#     def divide(self, x, y):
#         return x / y

#     def power(self, x, y):
#         return x ** y

# s = zerorpc.Server(caculate())

# s.bind("tcp://0.0.0.0:4242")
# print("SERVER: ", "tcp://0.0.0.0:4242")

# s.run()

# print("SERVER SUCCESS")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# import zerorpc

# class RPCServer(object):

#     def __init__(self):
#         super(RPCServer, self).__init__()
#         print(self)
#         self.send_data = "RPC Server"
#         self.recv_data = None

#     def getObj(self):
#         print("get data")
#         return self.send_data

#     def sendObj(self, data):
#         print("send data")
#         self.recv_data = data
#         print(self.recv_data)

# # zerorpc
# s = zerorpc.Server(RPCServer())

# print("SERVER: ", "tcp://0.0.0.0:4243")
# s.bind("tcp://0.0.0.0:4243")

# s.run()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import zerorpc

class MyRPCServer:

    def hello(self, name):
        """远程调用的函数示例"""
        print(f"Hello, {name}!")
        return f"Hello, {name}!"

    def process_data(self, data):
        """接收和处理复杂参数"""
        result = {}  # 初始化结果字典
        for key, value in data.items():
            result[key] = value * 2
        # print(result)
        return result


# 创建RPC服务器
server = zerorpc.Server(MyRPCServer())

# print("SERVER: ", "tcp://0.0.0.0:4242")
# server.bind("tcp://0.0.0.0:4242")

print("SERVER: ", "tcp://10.129.23.11:4244")
server.bind("tcp://10.129.23.11:4244")

# 启动服务器
server.run()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# import sys
# sys.set_int_max_str_digits(0)

# import json
# from http.server import HTTPServer, BaseHTTPRequestHandler
# from urllib.parse import urlparse, parse_qsl

# # host = ("127.0.0.1", 4244)
# host = ("10.211.55.2", 4244)

# class AddHandler(BaseHTTPRequestHandler):
#     def do_GET(self):
#         # 获取当前访问URL
#         current_url = urlparse(self.path)
#         # 获取URL携带的参数
#         query_args = dict(parse_qsl(current_url.query))
#         print(query_args)
#         arg1, arg2 = int(query_args.get("arg1", 1)), int(query_args.get("arg2", 1))
#         self.send_response(200)
#         self.send_header("content-type", "application/json")
#         self.end_headers()
#         self.wfile.write(json.dumps({"result": arg1 ** arg2},ensure_ascii=False).encode("utf-8"))


# if __name__ == '__main__':
#     server = HTTPServer(host, AddHandler)
#     print("启动服务器", host)
#     server.serve_forever()








