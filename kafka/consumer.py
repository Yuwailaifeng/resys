#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
from kafka import KafkaConsumer

# topic_name = "recommend_topic"
# kafka_addr = "10.129.31.22:9092,10.129.31.23:9092,10.129.31.24:9092"

consumer = KafkaConsumer(
    "recommend_topic",
    group_id="test_id",
    bootstrap_servers=[
        "10.129.31.22:9092",
        "10.129.31.23:9092",
        "10.129.31.24:9092",
    ],
    auto_offset_reset="earliest",
)

for msg in consumer:
    print(msg)
    print(f"topic = {msg.topic}")  # topic default is string
    print(f"partition = {msg.partition}")
    print(f"value = {msg.value.decode()}")  # bytes to string
    print(f"timestamp = {msg.timestamp}")
    print("time = ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg.timestamp / 1000)))
