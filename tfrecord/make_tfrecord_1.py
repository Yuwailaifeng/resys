#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("ORC File Reading") \
    .set("spark.executor.memory", "4g") \
    .set("spark.executor.cores", "4")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

textFile = sc.textFile("hdfs:///user/admin/an_zhong_xin/yt_recall/item2vec_1/20251028/20251028.content_album_for_recs.txt")
textFile.saveAsTextFile("file:///data/azx_reco/yt_recall/test.txt")

# df = spark.read.orc("/user/hive/warehouse/dm.db/recommend_user_behavior_detail/hours=20251026")

# df.show()
# df.printSchema()

# selected_df = df.select("column1", "column2")
# filtered_df = df.filter(df["column1"] > 10)
# filtered_df.show()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
