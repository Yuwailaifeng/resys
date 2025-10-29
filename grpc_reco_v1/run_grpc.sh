#!/bin/bash
# -*- coding: utf-8 -*-
# Authors: zhongxinan

#set -e

source /etc/profile
pyenv global 3.7.9
source ~/.bashrc

ls -lah ./

current_date=$(date +%Y-%m-%d\ %H:%M:%S)
echo "${current_date}"

ps aux | grep grpc_server | awk '{print $2}' | xargs kill -9

nohup python3 -u grpc_server_1.py  1>./log_1.txt 2>&1 &
nohup python3 -u grpc_server_2.py  1>./log_2.txt 2>&1 &

ps aux | grep grpc_server
echo "${current_date} gRPC ALL DONE!"
