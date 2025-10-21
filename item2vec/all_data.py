#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import os
import sys
from impala.dbapi import connect
import datetime
import pandas as pd
from collections import OrderedDict

print(sys.version_info)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

import heapq
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

impala_hour = 1
file_hour_list = []
for idx in range(impala_hour):
    file_hour = (datetime.datetime.now() - datetime.timedelta(days=impala_hour - idx)).date().strftime("%Y%m%d")
    file_hour_list.append(file_hour)
print(impala_hour, file_hour_list)

content_hour = 30
content_hour_list = []
for idx in range(content_hour):
    file_hour = (datetime.datetime.now() - datetime.timedelta(days=content_hour - idx)).date().strftime("%Y%m%d")
    content_hour_list.append(file_hour)
print(content_hour, content_hour_list)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

connection = connect(
    host="10.129.16.2",
    port=21050,
    # database="dw_v2",
)

cursor = connection.cursor()
cursor.execute("SELECT VERSION()")
print(f"Impala版本: {cursor.fetchone()[0]}")

try:
    with connection.cursor() as cursor:

        sql = """
            SELECT content_id_f, page_name, content_type, content_id, score, channel_id
            FROM dm.recommend_waterfall_v2_content_list
            WHERE ds = '""" + str(file_hour_list[-1]) + """' AND content_type = '专辑'
        """
        print(sql)

        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [item[0] for item in cursor.description]
        tempDf = pd.DataFrame(result, columns=columns)

        print("all_album_for_recs", file_hour_list[-1], len(tempDf), columns)
        tempDf.to_csv("./item2vec_1/model_data/" + str(file_hour_list[-1]) + ".all_album_for_recs.txt", header=columns, index=False, sep="\t", encoding="UTF-8")

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        sql = """
            SELECT content_id, page_name, content_type, content_id_f, score, channel_id
            FROM dm.recommend_waterfall_v2_content_list
            WHERE ds = '""" + str(file_hour_list[-1]) + """' AND content_type = '电台节目'
        """
        print(sql)

        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [item[0] for item in cursor.description]
        tempDf = pd.DataFrame(result, columns=columns)

        print("all_broadcast_for_recs", file_hour_list[-1], len(tempDf), columns)
        tempDf.to_csv("./item2vec_2/model_data/" + str(file_hour_list[-1]) + ".all_broadcast_for_recs.txt", header=columns, index=False, sep="\t", encoding="UTF-8")

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        sql = """
            SELECT content_id, page_name, content_type, content_id_f, score, channel_id
            FROM dm.recommend_waterfall_v2_content_list
            WHERE ds = '""" + str(file_hour_list[-1]) + """' AND content_type = '回听'
        """
        print(sql)

        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [item[0] for item in cursor.description]
        tempDf = pd.DataFrame(result, columns=columns)

        print("all_column_for_recs", file_hour_list[-1], len(tempDf), columns)
        tempDf.to_csv("./item2vec_3/model_data/" + str(file_hour_list[-1]) + ".all_column_for_recs.txt", header=columns, index=False, sep="\t", encoding="UTF-8")

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        sql = """
            SELECT content_id, page_name, content_type, content_id_f, score, channel_id
            FROM dm.recommend_waterfall_v2_content_list
            WHERE ds = '""" + str(file_hour_list[-1]) + """' AND content_type = '单曲'
        """
        print(sql)

        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [item[0] for item in cursor.description]
        tempDf = pd.DataFrame(result, columns=columns)

        print("all_single_for_recs", file_hour_list[-1], len(tempDf), columns)
        tempDf.to_csv("./item2vec_7/model_data/" + str(file_hour_list[-1]) + ".all_single_for_recs.txt", header=columns, index=False, sep="\t", encoding="UTF-8")

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

except Exception as e:
    print(f" Impala查询失败！原因：{str(e)}")

finally:
    cursor.close()
    connection.close()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
