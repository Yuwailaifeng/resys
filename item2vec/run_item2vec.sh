#!/bin/bash
# -*- coding: utf-8 -*-
# Authors: zhongxinan

set -e

source /etc/profile
pyenv global 3.7.9
source ~/.bashrc
if [ $? -ne 0 ]; then
    exit
fi


current_date=$(date +%Y-%m-%d\ %H:%M:%S)
current_date=$(date +%Y%m%d%H)
echo "${current_date}"
if [ $? -ne 0 ]; then
    exit
fi


cd /data/azx_reco/yt_recall/item2vec_1
python3 -u item2vec_1.py 1>>./log/${current_date}_log.txt 2>&1
if [ $? -ne 0 ]; then
    exit 1
fi


cd /data/azx_reco/yt_recall/item2vec_2
python3 -u item2vec_2.py 1>>./log/${current_date}_log.txt 2>&1
if [ $? -ne 0 ]; then
    exit 2
fi


cd /data/azx_reco/yt_recall/item2vec_3
python3 -u item2vec_3.py 1>>./log/${current_date}_log.txt 2>&1
if [ $? -ne 0 ]; then
    exit 3
fi


cd /data/azx_reco/yt_recall/item2vec_7
python3 -u item2vec_7.py 1>>./log/${current_date}_log.txt 2>&1
if [ $? -ne 0 ]; then
    exit 7
fi


cd /data/azx_reco/yt_recall
python3 -u content2redis.py 1>>./log/${current_date}_log.txt 2>&1
if [ $? -ne 0 ]; then
    exit
fi





#30 05 * * * cd /data/azx_reco/yt_recall && sh -x run_item2vec.sh 1>>./log/log.txt 2>&1
#ps aux | grep grpc_server | awk '{print $2}' | xargs kill -9
#python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./content.proto
#nohup python3 -u grpc_server.py  1>>./log.txt 2>&1 &






#count=0
#flag=1
#while [ $flag -ne 0 ]
#do
#    #current_date=$(date +%Y-%m-%d\ %H:%M:%S)
#    current_date=$(date +%Y%m%d%H)
#    echo ${current_date}
#    flag=$?
#
#    if [ $flag -eq 0 ]; then
#        break
#    else
#        count=$[${count}+1]
#        if [ ${count} -eq 3 ]; then
#            break
#        fi
#        sleep 2
#    fi
#done












echo "${current_date} Done!"
