#!/bin/bash
# -*- coding: utf-8 -*-
# Authors: zhongxinan

set -e

source /etc/profile
pyenv global 3.7.9
source ~/.bashrc


#current_date=$(date +%Y-%m-%d\ %H:%M:%S)
current_date=$(date +%Y%m%d%H)
echo "${current_date}"


#cd /data/azx_reco/yt_recall/item2vec_1
#python3 -u item2vec_1.py 1>>./log/${current_date}_log.txt 2>&1
#if [ $? -ne 0 ]; then
#    exit 1
#fi
#
#
#cd /data/azx_reco/yt_recall/item2vec_2
#python3 -u item2vec_2.py 1>>./log/${current_date}_log.txt 2>&1
#if [ $? -ne 0 ]; then
#    exit 2
#fi
#
#
#cd /data/azx_reco/yt_recall/item2vec_3
#python3 -u item2vec_3.py 1>>./log/${current_date}_log.txt 2>&1
#if [ $? -ne 0 ]; then
#    exit 3
#fi
#
#
#cd /data/azx_reco/yt_recall/item2vec_7
#python3 -u item2vec_7.py 1>>./log/${current_date}_log.txt 2>&1
#if [ $? -ne 0 ]; then
#    exit 7
#fi
#
#
#cd /data/azx_reco/yt_recall
#python3 -u content2redis.py 1>>./log/${current_date}_log.txt 2>&1
#if [ $? -ne 0 ]; then
#    exit
#fi


#30 05 * * * cd /data/azx_reco/yt_recall && sh -x run_item2vec.sh 1>>./log/$(date +"\%Y-\%m-\%d_\%H").log.txt 2>&1
#30 05 * * * cd /data/azx_reco/yt_recall && sh -x run_item2vec.sh 1>>./log/$(date +"\%Y\%m\%d\%H").log.txt 2>&1
#ps aux | grep grpc_server | awk '{print $2}' | xargs kill -9
#ps aux | grep item2vec | awk '{print $2}' | xargs kill -9
#python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./content.proto
#nohup python3 -u grpc_server.py  1>>./log.txt 2>&1 &


command_1="cd /data/azx_reco/yt_recall/item2vec_1"
command_2="python3 -u item2vec_1.py >>./item2vec_1/log/${current_date}_log.txt"
retries=10
count=0
while true; do
    echo ${command_1} ${command_2}
    ${command_1} && ${command_2} && break
    if [ $count -eq $retries ]; then
        echo "EXIT" $(date +%Y-%m-%d\ %H:%M:%S)
        exit
    fi
    echo "Retry..." $(date +%Y-%m-%d\ %H:%M:%S)
    sleep 900
    count=$((count+1))
    echo ${count}
done
echo ${command_1} ${command_2} "SUCCESS" $(date +%Y-%m-%d\ %H:%M:%S)


command_1="cd /data/azx_reco/yt_recall/item2vec_7"
command_2="python3 -u item2vec_7.py >>./item2vec_7/log/${current_date}_log.txt"
retries=10
count=0
while true; do
    echo ${command_1} ${command_2}
    ${command_1} && ${command_2} && break
    if [ $count -eq $retries ]; then
        echo "EXIT" $(date +%Y-%m-%d\ %H:%M:%S)
        exit
    fi
    echo "Retry..." $(date +%Y-%m-%d\ %H:%M:%S)
    sleep 900
    count=$((count+1))
    echo ${count}
done
echo ${command_1} ${command_2} "SUCCESS" $(date +%Y-%m-%d\ %H:%M:%S)


command_1="cd /data/azx_reco/yt_recall"
command_2="python3 -u content2redis.py >>./log/${current_date}_log.txt"
retries=10
count=0
while true; do
    echo ${command_1} ${command_2}
    ${command_1} && ${command_2} && break
    if [ $count -eq $retries ]; then
        echo "EXIT" $(date +%Y-%m-%d\ %H:%M:%S)
        exit
    fi
    echo "Retry..." $(date +%Y-%m-%d\ %H:%M:%S)
    sleep 900
    count=$((count+1))
    echo ${count}
done
echo ${command_1} ${command_2} "SUCCESS" $(date +%Y-%m-%d\ %H:%M:%S)


echo "${current_date} DONE!"
