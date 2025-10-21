#!/bin/bash
# -*- coding: utf-8 -*-
# Authors: zhongxinan

source /etc/profile
pyenv global 3.7.9
source ~/.bashrc

#current_date=$(date +%Y-%m-%d\ %H:%M:%S)
current_date=$(date +%Y%m%d%H)
echo ${current_date}

cd /data/azx_reco/yt_recall/item2vec_1
python3 -u item2vec_1.py 1>>./log/${current_date}_log.txt 2>&1

cd /data/azx_reco/yt_recall/item2vec_2
python3 -u item2vec_2.py 1>>./log/${current_date}_log.txt 2>&1

cd /data/azx_reco/yt_recall/item2vec_3
python3 -u item2vec_3.py 1>>./log/${current_date}_log.txt 2>&1

cd /data/azx_reco/yt_recall/item2vec_7
python3 -u item2vec_7.py 1>>./log/${current_date}_log.txt 2>&1

cd /data/azx_reco/yt_recall
python3 -u content2redis.py 1>>./${current_date}_log.txt 2>&1

#01 05 * * * cd /data/azx_reco/yt_recall && sh -x run_item2vec.sh 1>>./$(date +%Y%m%d%H)_log.txt 2>&1

