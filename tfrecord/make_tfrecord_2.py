#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder \
    .appName("spark_sql") \
    .config("spark.sql.warehouse.dir", "hdfs://10.129.16.33:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://10.129.16.33:9083") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

spark.sql("show databases").show()
spark.sql("show databases").show()
spark.sql("use sparkhive").show()
spark.sql("show tables").show()

# conf = SparkConf().setAppName("HDFS File Reading") \
#     .set("spark.driver.extraJavaOptions", "-Dlog4j.logLevel=ERROR") \
#     .set("spark.executor.extraJavaOptions", "-Dlog4j.logLevel=ERROR") \
#     .set("spark.executor.memory", "10g") \
#     .set("spark.executor.cores", "10")
# sc = SparkContext(conf=conf)
# spark = SparkSession(sc)
# spark.sparkContext.setLogLevel("ERROR")
#
# column_name = [
#     "device_uuid",
#     "user_id",
#     "act_ls",
#     "act_cli",
#     "fea_sex",
#     "fea_age",
#     "fea_income",
#     "fea_culture",
#     "fea_sex_busi",
#     "fea_age_busi",
#     "context_operatorid",
#     "context_mode",
#     "context_devicetype",
#     "context_redevicenetworktype",
#     "context_redevicemodel",
#     "context_redevicemanufacturer",
#     "context_signalstrength",
#     "content_type",
#     "content_type_name",
#     "listen_duration",
#     "song_album_id",
#     "song_single_name",
#     "song_media_type",
#     "song_des_type",
#     "song_duration",
#     "song_publish_time",
#     "song_sort_no",
#     "song_play_url_low",
#     "song_play_url_high",
#     "song_play_url_source",
#     "song_download_url",
#     "song_file_size",
#     "song_listen_count",
#     "song_audition_flag",
#     "song_price",
#     "song_price_unit",
#     "song_info_edit_time",
#     "song_info_edit_by",
#     "song_enable_status",
#     "song_enable_status_oper_type",
#     "song_first_enable_time",
#     "song_auto_disable_time",
#     "song_unable_reason",
#     "song_delete_flag",
#     "song_created_by",
#     "song_created_time",
#     "song_updated_by",
#     "song_updated_time",
#     "album_album_name",
#     "album_des_type",
#     "album_sort_type",
#     "album_stereo_type",
#     "album_listen_count",
#     "album_single_count",
#     "album_comment_count",
#     "album_end_flag",
#     "album_vip_flag",
#     "album_fee_type",
#     "album_audition_type",
#     "album_price",
#     "album_price_unit",
#     "album_expire_time",
#     "album_level_type",
#     "album_ugc_flag",
#     "album_broadcast_id",
#     "album_last_pub_time",
#     "album_enable_status",
#     "album_auto_disable_day",
#     "album_unable_reason",
#     "album_special_flag",
#     "album_info_edit_time",
#     "album_info_edit_by",
#     "album_delete_flag",
#     "album_created_by",
#     "album_created_time",
#     "album_updated_by",
#     "album_updated_time",
#     "album_publish_time",
#     "album_des_simple",
#     "album_des",
#     "freq_broadcast_name",
#     "freq_sort_no",
#     "freq_listen_back_flag",
#     "freq_fm",
#     "freq_am",
#     "freq_level_type",
#     "freq_province",
#     "freq_city",
#     "freq_district",
#     "freq_province_code",
#     "freq_city_code",
#     "freq_district_code",
#     "freq_began_time",
#     "freq_source_type",
#     "freq_url_source_type",
#     "freq_time_shifted_flag",
#     "freq_page_flag",
#     "freq_liveroom_type",
#     "freq_listen_count",
#     "freq_search_status",
#     "freq_enable_status",
#     "freq_remark",
#     "freq_delete_flag",
#     "freq_created_by",
#     "freq_created_time",
#     "freq_updated_by",
#     "freq_updated_time",
#     "program_column_id",
#     "program_broadcast_id",
#     "program_program_name",
#     "program_des",
#     "program_start_time",
#     "program_end_time",
#     "program_source_type",
#     "program_time_shifted_flag",
#     "program_program_date",
#     "program_play_url_low",
#     "program_play_url_high",
#     "program_play_flag",
#     "program_listen_count",
#     "program_comment_count",
#     "program_enable_status",
#     "program_enable_status_oper_type",
#     "program_first_enable_time",
#     "program_auto_disable_time",
#     "program_delete_flag",
#     "program_created_by",
#     "program_created_time",
#     "program_updated_by",
#     "program_updated_time",
#     "column_column_name",
#     "column_broadcast_id",
#     "column_listen_count",
#     "column_program_count",
#     "column_comment_count",
#     "column_last_pub_time",
#     "column_search_status",
#     "column_enable_status",
#     "column_auto_disable_day",
#     "column_unable_reason",
#     "column_delete_flag",
#     "column_created_by",
#     "column_created_time",
#     "column_updated_by",
#     "column_updated_time",
#     "liveb_anchor_id",
#     "liveb_talk_room_id",
#     "liveb_broadcast_id",
#     "liveb_live_room_name",
#     "liveb_topic",
#     "liveb_live_type",
#     "liveb_start_time",
#     "liveb_end_time",
#     "liveb_live_status",
#     "liveb_check_status",
#     "liveb_check_result",
#     "liveb_delete_flag",
#     "liveb_created_by",
#     "liveb_created_time",
#     "liveb_updated_by",
#     "liveb_updated_time",
#     "liveb_owner_tenant",
#     "liveb_anchor_owner_dept_id",
#     "liveb_source_id",
#     "liveb_live_source_type",
#     "liveb_real_start_time",
#     "liveb_pay_type",
# ]

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# df = sc.textFile("hdfs://10.129.16.33:8020/user/admin/an_zhong_xin/yt_recall/item2vec_1/20251028/20251028.content_album_for_recs.txt")
# print("df.count()", df.count())
# # print("df.first()", df.first())
# tmp_rdd = df.take(10)
# for idx, item in enumerate(tmp_rdd):
#     print(idx, item)
#
# # df.saveAsTextFile("hdfs://10.129.16.33:8020/user/admin/an_zhong_xin/yt_recall/item2vec_1/test.txt")
# # df.saveAsTextFile("file:///data/azx_reco/yt_recall/test")
#
# # df = spark.read.orc("hdfs://10.129.16.33:8020/user/hive/warehouse/dm.db/recommend_user_behavior_detail/hours=20251026/*")
# # df.printSchema()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# df = sc.textFile("hdfs://10.129.16.33:8020/user/hive/warehouse/dm.db/recommend_user_behavior_detail/hours=20251026")
# print("df.count()", df.count())
# # df.printSchema()
# # # print("df.first()", df.first())
# #
# tmp_rdd = df.take(10)
# for idx, item in enumerate(tmp_rdd):
#     print(idx, item)

# tmp_list = list(map(lambda line: line.split("\t"), tmp_rdd))
# for idx, item in enumerate(tmp_list):
#     print(idx, len(item), item)
#
# res_rdd = spark.createDataFrame(tmp_list, schema=column_name)
# tmp_rdd = res_rdd.take(10)
# for idx, item in enumerate(tmp_rdd):
#     print(idx, item)
# # res_rdd.saveAsTextFile("hdfs://10.129.16.33:8020/user/admin/an_zhong_xin/yt_recall/item2vec_1/test.txt")
#
#
# # selected_df = df.select("column1", "column2")
# # filtered_df = df.filter(df["column1"] > 10)
# # filtered_df.show()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# sqlContext = SQLContext(sc)
#
# df = sqlContext.read.table("hive_tbl")
#
# # df.filter("age > 30").select("name", "age").show()
#
# df.show(10)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
