#!/bin/bash
# -*- coding: utf-8 -*-
# Authors: zhongxinan

#set -e

source /etc/profile
pyenv global 3.7.9
source ~/.bashrc

ls -lah ./

current_date=$(date +%Y%m%d%H)
echo "${current_date}"

ps aux | grep kafka_1 | awk '{print $2}' | xargs kill -9

nohup python3 -u kafka_1.py  1>>./log/${current_date}_log_1.txt 2>&1 &

ps aux | grep kafka_1
echo "${current_date} KAFKA DONE!"
