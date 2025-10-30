#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("ORC File Reading") \
    .set("spark.executor.memory", "10g") \
    .set("spark.executor.cores", "10")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

df = sc.textFile("hdfs://10.129.16.33:8020/user/admin/an_zhong_xin/yt_recall/item2vec_1/20251028/20251028.content_album_for_recs.txt")
print("df.count()", df.count())
print("df.first()", df.first())
tmp_rdd = df.take(10)
for idx, item in enumerate(tmp_rdd):
    print(idx, item)

# df.saveAsTextFile("hdfs://10.129.16.33:8020/user/admin/an_zhong_xin/yt_recall/item2vec_1/test.txt")
df.saveAsTextFile("file:///data/azx_reco/yt_recall/test")

# df = spark.read.orc("hdfs://10.129.16.33:8020/user/hive/warehouse/dm.db/recommend_user_behavior_detail/hours=20251026/*")
# df.printSchema()

df = sc.textFile("hdfs://10.129.16.33:8020/user/hive/warehouse/dm.db/recommend_user_behavior_detail/hours=20251026")
print("df.count()", df.count())
print("df.first()", df.first())

tmp_rdd = df.take(10)
for idx, item in enumerate(tmp_rdd):
    print(idx, item)


# selected_df = df.select("column1", "column2")
# filtered_df = df.filter(df["column1"] > 10)
# filtered_df.show()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
