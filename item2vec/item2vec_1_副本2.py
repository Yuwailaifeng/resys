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

sql_list = []
for file_hour in file_hour_list:
    # sql_list.append("""SELECT `device_uuid`, GROUP_CONCAT(DISTINCT CONCAT(CAST(`content_type` AS STRING), "|", CAST(`content_id_c` AS STRING)), ";") content_id_sequence FROM `dw_v2`.`user_ls_day_summary` WHERE `ds` = '""" + str(file_hour) + """' AND `content_type` = 1 AND `listen_dur_sum` > 3000 AND `device_uuid` IS NOT NULL AND `content_id_c` IS NOT NULL GROUP BY `device_uuid`""")
    sql_list.append("""
        SELECT `device_uuid`, GROUP_CONCAT(DISTINCT CONCAT(CAST(`content_type` AS STRING), "|", CAST(`content_id_c` AS STRING)), ";") content_id_sequence
        FROM `dw_v2`.`user_ls_day_summary`
        WHERE `ds` = '""" + str(file_hour) + """' AND `content_type` = 1 AND `device_uuid` IS NOT NULL AND `content_id_c` IS NOT NULL
        GROUP BY `device_uuid`
    """)

try:
    with connection.cursor() as cursor:
        # # sql = """SELECT CONCAT(CAST(`content_type` AS STRING), "|", `content_type_name`)  content, COUNT(*) content_count FROM `dw_v2`.`user_ls_day_summary` WHERE `ds` = '20251009' GROUP BY `content` ORDER BY `content_count` DESC"""
        # # sql = """SELECT * FROM `dm`.`recommend_waterfall_v2_content_list` WHERE `ds` = '""" + str(file_hour_list[-1]) + """' AND `content_type` = '专辑'"""
        # # sql = """SELECT DISTINCT `content_id_c`, `content_name_f` FROM `dw_v2`.`user_ls_day_summary` WHERE `ds` = '20251013' AND `content_type` = 1"""
        # # sql = """SELECT `id`, `album_name`, `listen_count` FROM `dict`.`content_basic_2_dict_t_album` WHERE `listen_count` >= 0"""
        # sql = """SELECT * FROM `dict`.`content_basic_2_dict_t_album` WHERE `enable_status` = 1 AND `delete_flag` = 0"""
        # print(sql)
        #
        # cursor.execute(sql)
        # result = cursor.fetchall()
        # columns = [item[0] for item in cursor.description]
        # tempDf = pd.DataFrame(result, columns=columns)
        #
        # print("`dict`.`content_basic_2_dict_t_album`", file_hour_list[-1], len(tempDf), columns)
        # tempDf.to_csv("./impala_data/" + str(file_hour_list[-1]) + ".content_basic_2_dict_t_album.txt", header=columns, index=False, sep="\t", encoding="UTF-8")

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        sql = """
            SELECT t1.content_id_f, t1.page_name, t2.album_name, t1.content_type, t1.channel_id
            FROM dm.recommend_waterfall_v2_content_list t1
            LEFT JOIN dict.content_basic_2_dict_t_album_for_recs t2
            ON t1.content_id_f = t2.id
            WHERE t1.ds = '""" + str(file_hour_list[-1]) + """' AND t1.content_type = '专辑'
        """
        print(sql)

        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [item[0] for item in cursor.description]
        tempDf = pd.DataFrame(result, columns=columns)

        print("content_album_for_recs", file_hour_list[-1], len(tempDf), columns)
        tempDf.to_csv("./model_data/" + str(file_hour_list[-1]) + ".content_album_for_recs.txt", header=columns, index=False, sep="\t", encoding="UTF-8")

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        for file_hour, sql in zip(file_hour_list, sql_list):
            print(sql)
            cursor.execute(sql)

            result = cursor.fetchall()
            columns = [item[0] for item in cursor.description]
            tempDf = pd.DataFrame(result, columns=columns)

            # tempDf[columns[1]] = tempDf[columns[1]].replace("\t", "")
            # tempDf[columns[1]] = tempDf[columns[1]].replace({
            #     "\t": "",
            #     " ": "",
            # })

            # print(tempDf)
            print("`dw_v2`.`user_ls_day_summary`", file_hour, len(tempDf), columns)
            tempDf.to_csv("./impala_data/" + str(file_hour) + ".device_uuid_content_id.txt", header=False, index=False, sep="\t", encoding="UTF-8")

            # for row in result:
            #     print(row)
            # print()

except Exception as e:
    print(f" Impala查询失败！原因：{str(e)}")

finally:
    cursor.close()
    connection.close()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

id_name_dict = {}
page_name_dict = {}
channel_id_dict = {}
with open("./model_data/" + str(file_hour_list[-1]) + ".content_album_for_recs.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        line = line.strip().split("\t")
        page_name_dict.setdefault(line[0], [])
        channel_id_dict.setdefault(line[0], [])
        if line[1] not in page_name_dict[line[0]]:
            page_name_dict[line[0]].append(line[1])
            channel_id_dict[line[0]].append(line[-1])
        id_name_dict[line[0]] = "#".join(page_name_dict[line[0]]) + "|" + line[2] + "|" + "#".join(channel_id_dict[line[0]])
print("len(id_name_dict)", len(id_name_dict))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

user_content_sequence_dict = {}
for file_hour in content_hour_list:
    with open("./impala_data/" + str(file_hour) + ".device_uuid_content_id.txt", encoding="UTF-8") as file:
        for line in file.readlines():
            tmp = line.strip().split("\t")
            device_uuid, content_sequence = tmp[0], tmp[1]
            content_list = content_sequence.strip().split(";")
            content_list = [item.split("|")[1] for item in content_list if item.split("|")[1] in id_name_dict]
            for content in content_list:
                content = content.strip()
                if len(content) > 0:
                    user_content_sequence_dict.setdefault(device_uuid, [])
                    user_content_sequence_dict[device_uuid].append(content)
    print("len(user_content_sequence_dict)", len(user_content_sequence_dict), file_hour)

with open("./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence.txt", "w", encoding="UTF-8") as file:
    for device_uuid, content_list in user_content_sequence_dict.items():
        unique_list = list(OrderedDict.fromkeys(content_list[::-1]))
        if len(unique_list) > 0:
            file.write(device_uuid + "\t" + ";".join(unique_list) + "\n")

with open("./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence_for_rec.txt", "w", encoding="UTF-8") as file:
    for device_uuid, content_list in user_content_sequence_dict.items():
        if len(content_list) > 1:
            unique_list = list(OrderedDict.fromkeys(content_list))
            if len(unique_list) > 1:
                file.write(" ".join(unique_list) + "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

content_count_dict = {}
for file_hour in content_hour_list[-1:]:
    print("./impala_data/" + str(file_hour) + ".device_uuid_content_id.txt")
    with open("./impala_data/" + str(file_hour) + ".device_uuid_content_id.txt", encoding="UTF-8") as file:
        for line in file.readlines():
            tmp = line.strip().split("\t")
            device_uuid, content_sequence = tmp[0], tmp[1]
            content_list = content_sequence.strip().split(";")
            content_list = [item.split("|")[1] for item in content_list if item.split("|")[1] in id_name_dict]
            unique_list = list(OrderedDict.fromkeys(content_list))

            for content in content_list:
                content = content.strip()
                if len(content) > 0:
                    content_count_dict.setdefault(content, 0)
                    content_count_dict[content] += 1
    print("len(content_count_dict)", len(content_count_dict), file_hour)

content_count_sort = sorted(content_count_dict.items(), key=lambda x: x[1], reverse=True)
print("len(content_count_sort)", len(content_count_sort))

for i in range(10):
    print(i, content_count_sort[i], id_name_dict[content_count_sort[i][0]])

with open("./model_data/" + str(content_hour_list[-1]) + ".content_count.txt", "w", encoding="UTF-8") as file:
    for key, value in content_count_sort:
        file.write(key + "\t" + str(value) + "\t" + id_name_dict[key] + "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

os.system("ls -la ./impala_data")
os.system("time ./word2vec -train ./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence_for_rec.txt -output ./model_data/" + str(content_hour_list[-1]) + ".vectors.cbow.txt -cbow 1 -size 100 -window 8 -negative 25 -hs 0 -sample 1e-4 -threads 10 -binary 0 -iter 30")
os.system("ls -la ./sample_data")
os.system("time ./word2vec -train ./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence_for_rec.txt -output ./model_data/" + str(content_hour_list[-1]) + ".vectors.cbow.binary -cbow 1 -size 100 -window 8 -negative 25 -hs 0 -sample 1e-4 -threads 10 -binary 1 -iter 30")
os.system("ls -la ./model_data")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def matrix_cosine(matrix1, matrix2):
    return cosine_similarity(matrix1, matrix2)


def i2i_save_file(similarity_score_list, key_list, id_name_dict, vectors_similarity_file, vectors_similarity_name_file):
    top_k = min(3000, len(similarity_score_list))
    print("len(similarity_score_list)", len(similarity_score_list))
    with open(vectors_similarity_file, "w", encoding="UTF-8") as file:
        for query_idx in range(len(key_list)):
            similarity_idx = list(map(similarity_score_list[query_idx].index, heapq.nlargest(top_k, similarity_score_list[query_idx])))
            # similarity_value = heapq.nlargest(top_k, similarity_score_list[query_idx])
            res = [key_list[idx] + "|" + id_name_dict[key_list[idx]].split("|")[-1] for idx in similarity_idx]
            # res_name = [key_list[idx] + "|" + id_name_dict[key_list[idx]] for idx in similarity_idx]
            file.write(key_list[query_idx] + "|" + id_name_dict[key_list[query_idx]].split("|")[-1] + "\t" + ";".join(res[1:]) + "\n")

    with open(vectors_similarity_name_file, "w", encoding="UTF-8") as file:
        for query_idx in range(len(key_list)):
            similarity_idx = list(map(similarity_score_list[query_idx].index, heapq.nlargest(top_k, similarity_score_list[query_idx])))
            # similarity_value = heapq.nlargest(top_k, similarity_score_list[query_idx])
            # res = [key_list[idx] + "|" + id_name_dict[key_list[idx]].split("|")[1] for idx in similarity_idx]
            res_name = [key_list[idx] + "|" + id_name_dict[key_list[idx]] for idx in similarity_idx]
            file.write(key_list[query_idx] + "|" + id_name_dict[key_list[query_idx]] + "\t" + " ; ".join(res_name[1:]) + "\n")


key_list = []
vector_list = []
with open("./model_data/" + str(content_hour_list[-1]) + ".vectors.cbow.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        try:
            line = line.strip().split(" ")
            if len(line) < 3 or line[0] == "</s>":
                continue
            key_list.append(line[0])
            vector_list.append(np.array([float(value) for value in line[1:]], dtype=np.float32))
        except:
            print(line)

# print(key_list[:100])
print(len(key_list))
print(len(vector_list))
print(len(vector_list[1]))

for i in range(len(vector_list)):
    if len(vector_list[i]) != 100:
        print(i, key_list[i], len(vector_list[i]))

vector_array = np.array(vector_list, dtype=np.float32)
print("vector_array.shape", vector_array.shape)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

similarity_v1 = matrix_cosine(vector_array, vector_array)
print("similarity_v1.shape", similarity_v1.shape)

similarity_score_list = similarity_v1.tolist()
print("len(similarity_score_list)", len(similarity_score_list))

i2i_save_file(similarity_score_list, key_list, id_name_dict, "./model_data/" + str(content_hour_list[-1]) + ".vectors_similarity.txt", "./model_data/" + str(content_hour_list[-1]) + ".vectors_similarity_name.txt")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def u2i_save_file(user_content_id_reco_dict, id_name_dict, vectors_similarity_file, vectors_similarity_name_file, user_his_dict):
    print("len(user_content_id_reco_dict)", len(user_content_id_reco_dict))
    with open(vectors_similarity_file, "w", encoding="UTF-8") as file:
        for key, value in user_content_id_reco_dict.items():
            res_name = [k + "|" + id_name_dict[k].split("|")[-1] for k in value]
            file.write(key + "\t" + ";".join(res_name) + "\n")

    with open(vectors_similarity_name_file, "w", encoding="UTF-8") as file:
        for key, value in user_content_id_reco_dict.items():
            his_res_name = [k + "|" + id_name_dict[k] for k in user_his_dict[key]]
            res_name = [k + "|" + id_name_dict[k] for k in value]
            file.write(key + "\n" + " ; ".join(his_res_name) + "\n" + " ; ".join(res_name) + "\n\n")


content_id_similarity_dict = {}
with open("./model_data/" + str(content_hour_list[-1]) + ".vectors_similarity.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        line = line.strip().split("\t")
        content_id_similarity_dict[line[0].split("|")[0]] = [item.split("|")[0] for item in line[1].split(";")]
print("len(content_id_similarity_dict)", len(content_id_similarity_dict))

n = 0
user_his_dict = {}
with open("./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        line = line.strip().split("\t")
        if len(line) != 2:
            print(len(line), line)
            continue
        content_id_list = [item for item in line[1].split(";") if item in content_id_similarity_dict]
        unique_list = list(OrderedDict.fromkeys(content_id_list))
        if len(content_id_list) > 0 and len(content_id_list) == len(unique_list):
            user_his_dict[line[0]] = unique_list
        else:
            n += 1
            # print(line, len(content_id_list), len(content_id_list), len(unique_list), content_id_list, unique_list)
        # if len(user_his_dict) >= 10000:
        #     break

# for k, v in user_his_dict.items():
#     print(k, v)

print("len(user_his_dict)", len(user_his_dict), n)

user_content_id_reco_dict = {}
for key, value in user_his_dict.items():
    user_content_id_reco_dict.setdefault(key, [])
    for idx in range(len(content_id_similarity_dict) - 1):
        if len(user_content_id_reco_dict[key]) >= 100:
            break
        for item in value:
            if item in content_id_similarity_dict and content_id_similarity_dict[item][idx] not in value:
                user_content_id_reco_dict[key].append(content_id_similarity_dict[item][idx])

# for k, v in user_content_id_reco_dict.items():
#     if len(v) < 100:
#         print(k, len(v), user_his_dict[k])

print("len(user_content_id_reco_dict)", len(user_content_id_reco_dict))

u2i_save_file(user_content_id_reco_dict, id_name_dict, "./model_data/" + str(content_hour_list[-1]) + ".user_vectors_similarity.txt", "./model_data/" + str(content_hour_list[-1]) + ".user_vectors_similarity_name.txt", user_his_dict)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

with open("./model_data/" + str(content_hour_list[-1]) + ".user_seq_check.txt", "w", encoding="UTF-8") as file:
    for key, value in user_content_sequence_dict.items():
        if key not in user_his_dict:
            flag_list = [item + "|1" if item in content_id_similarity_dict else item + "|0" for item in value]
            file.write(key + "\t" + " ; ".join(flag_list) + "\n")

print("len(user_content_sequence_dict)", len(user_content_sequence_dict))
print("len(user_his_dict)", len(user_his_dict))
print("len(user_content_id_reco_dict)", len(user_content_id_reco_dict))

os.system("head -n 1000 " + "./model_data/" + str(content_hour_list[-1]) + ".user_vectors_similarity_name.txt" + " > ./model_data/user_vectors_similarity_name.txt")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# # zip -r item2vec.zip item2vec -x "*.txt"
#
# 01  09 * * * source /etc/profile && pyenv global 3.7.9 && source ~/.bashrc && cd /data/azx_reco/yt_recall/item2vec_1 && /home/ubuntu/.pyenv/versions/3.7.9/bin/python3 -u ./item2vec_1.py 1>>./log.txt 2>&1

os.system("ls -lah ./impala_data")
os.system("ls -lah ./sample_data")
os.system("ls -lah ./model_data")

hdfs_file_path = "/user/admin/an_zhong_xin/yt_recall/item2vec_1/" + str(content_hour_list[-1] + "/")
print("hdfs_file_path", hdfs_file_path)

print("hadoop fs -mkdir " + hdfs_file_path)
os.system("hadoop fs -mkdir " + hdfs_file_path)

print("hadoop fs -put -f ./model_data/" + str(content_hour_list[-1]) + "* " + hdfs_file_path)
os.system("hadoop fs -put -f ./model_data/" + str(content_hour_list[-1]) + "* " + hdfs_file_path)

print("hadoop fs -ls " + hdfs_file_path)
os.system("hadoop fs -ls -h " + hdfs_file_path)

pre_day = (datetime.datetime.now() + datetime.timedelta(days=-9)).date().strftime("%Y%m%d")
print("rm -rf ./model_data/" + str(pre_day) + ".user_vectors_similarity_name.txt")
os.system("rm -rf ./model_data/" + str(pre_day) + ".user_vectors_similarity_name.txt")

print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print(str(content_hour_list[-1]) + " DONE!")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
