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

Topk = 101

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

impala_hour = 31
file_hour_list = []
for idx in range(impala_hour):
    file_hour = (datetime.datetime.now() + datetime.timedelta(days=idx - impala_hour)).date().strftime("%Y%m%d")
    file_hour_list.append(file_hour)
print(file_hour_list, file_hour_list)

content_hour = 31
content_hour_list = []
for idx in range(content_hour):
    file_hour = (datetime.datetime.now() + datetime.timedelta(days=idx - content_hour)).date().strftime("%Y%m%d")
    content_hour_list.append(file_hour)
print(content_hour, content_hour_list)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# connection = connect(
#     host="10.129.16.2",
#     port=21050,
#     # database="dw_v2",
# )
#
# cursor = connection.cursor()
# cursor.execute("SELECT VERSION()")
# print(f"Impala版本: {cursor.fetchone()[0]}")
#
# sql_list = []
# for file_hour in file_hour_list:
#     sql_list.append("""SELECT `device_uuid`, GROUP_CONCAT(DISTINCT CONCAT(CAST(`content_type` AS STRING), "|", CAST(`content_id_c` AS STRING)), ";") content_id_sequence FROM `dw_v2`.`user_ls_day_summary` WHERE `ds` = '""" + str(file_hour) + """' AND `content_type` = 1 AND `listen_count` > 0 AND `device_uuid` IS NOT NULL AND `content_id_c` IS NOT NULL GROUP BY `device_uuid`""")
# # print(sql_list)
#
# try:
#     with connection.cursor() as cursor:
#         # sql = """SELECT CONCAT(CAST(`content_type` AS STRING), "|", `content_type_name`)  content, COUNT(*) content_count FROM `dw_v2`.`user_ls_day_summary` WHERE `ds` = '20251009' GROUP BY `content` ORDER BY `content_count` DESC"""
#         # sql = """SELECT * FROM `dm`.`recommend_waterfall_v2_content_list` WHERE `ds` = '""" + str(file_hour_list[-1]) + """' AND `content_type` = '专辑'"""
#         # sql = """SELECT DISTINCT `content_id_c`, `content_name_f` FROM `dw_v2`.`user_ls_day_summary` WHERE `ds` = '20251013' AND `content_type` = 1"""
#         # sql = """SELECT `id`, `album_name`, `listen_count` FROM `dict`.`content_basic_2_dict_t_album` WHERE `listen_count` >= 0"""
#         sql = """SELECT * FROM `dict`.`content_basic_2_dict_t_album`"""
#         print(sql)
#
#         cursor.execute(sql)
#         result = cursor.fetchall()
#         columns = [item[0] for item in cursor.description]
#         tempDf = pd.DataFrame(result, columns=columns)
#
#         print(file_hour_list[-1], len(tempDf), columns)
#         tempDf.to_csv("./impala_data/" + str(file_hour_list[-1]) + ".content_basic_2_dict_t_album.txt", header=columns, index=False, sep="\t", encoding="UTF-8")
#
#         for file_hour, sql in zip(file_hour_list, sql_list):
#             # print(sql)
#             cursor.execute(sql)
#
#             result = cursor.fetchall()
#             columns = [item[0] for item in cursor.description]
#             tempDf = pd.DataFrame(result, columns=columns)
#
#             # tempDf[columns[1]] = tempDf[columns[1]].replace("\t", "")
#             # tempDf[columns[1]] = tempDf[columns[1]].replace({
#             #     "\t": "",
#             #     " ": "",
#             # })
#
#             # print(tempDf)
#             print(file_hour, len(tempDf), columns)
#             tempDf.to_csv("./impala_data/" + str(file_hour) + ".device_uuid_content_id.txt", header=False, index=False, sep="\t", encoding="UTF-8")
#
#             # for row in result:
#             #     print(row)
#             # print()
#
# except Exception as e:
#     print(f" Impala查询失败！原因：{str(e)}")
#
# finally:
#     cursor.close()
#     connection.close()
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# user_content_sequence_dict = {}
# for file_hour in content_hour_list:
#     with open("./impala_data/" + str(file_hour) + ".device_uuid_content_id.txt", encoding="UTF-8") as file:
#         for line in file.readlines():
#             tmp = line.strip().split("\t")
#             device_uuid, content_sequence = tmp[0], tmp[1]
#             content_list = content_sequence.strip().split(";")
#             user_content_sequence_dict.setdefault(device_uuid, [])
#             for content in content_list:
#                 content = content.strip()
#                 if len(content) > 0 and content not in user_content_sequence_dict[device_uuid]:
#                     user_content_sequence_dict[device_uuid].append(content)
#     print("len(user_content_sequence_dict)", len(user_content_sequence_dict), file_hour)
#
# with open("./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence.txt", "w", encoding="UTF-8") as file:
#     for device_uuid, content_list in user_content_sequence_dict.items():
#         file.write(device_uuid + "\t" + ";".join(content_list) + "\n")
#
# with open("./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence_for_rec.txt", "w", encoding="UTF-8") as file:
#     for device_uuid, content_list in user_content_sequence_dict.items():
#         if len(content_list) > 1:
#             content_id_list = [item.split("|")[1] for item in content_list]
#             unique_list = list(OrderedDict.fromkeys(content_id_list))
#             if len(unique_list) > 1:
#                 file.write(" ".join(unique_list) + "\n")
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# os.system("ls -la ./impala_data")
# os.system("time ./word2vec -train ./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence_for_rec.txt -output ./model_data/" + str(content_hour_list[-1]) + ".vectors.cbow.txt -cbow 1 -size 100 -window 8 -negative 25 -hs 0 -sample 1e-4 -threads 10 -binary 0 -iter 90")
# os.system("ls -la ./sample_data")
# os.system("time ./word2vec -train ./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence_for_rec.txt -output ./model_data/" + str(content_hour_list[-1]) + ".vectors.cbow.binary -cbow 1 -size 100 -window 8 -negative 25 -hs 0 -sample 1e-4 -threads 10 -binary 1 -iter 90")
# os.system("ls -la ./model_data")
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def matrix_cosine(matrix1, matrix2):
    return cosine_similarity(matrix1, matrix2)


def i2i_save_file(Topk, similarity_score_list, key_list, id_name_dict, vectors_similarity_file, vectors_similarity_name_file):
    print("len(similarity_score_list)", len(similarity_score_list))
    for query_idx in range(len(key_list)):
        res = similarity_score_list[query_idx].tolist()
        res_idx = list(map(res.index, heapq.nlargest(Topk, res)))
        res_value = heapq.nlargest(Topk, res)
        if key_list[query_idx] not in id_name_dict:
            continue
        # res = [str(idx) + "|" + key_list[idx] + "|" + str(value) for idx, value in zip(res_idx, res_value)]
        res = [key_list[idx] for idx, value in zip(res_idx, res_value) if key_list[idx] in id_name_dict]
        res_name = [id_name_dict[key] for key in res]
        if query_idx == 0:
            with open(vectors_similarity_file, "w", encoding="UTF-8") as file1:
                file1.write(str(query_idx) + "|" + key_list[query_idx] + "\t" + res[0] + "|" + "|".join(res[1:]) + "\n")
            with open(vectors_similarity_name_file, "w", encoding="UTF-8") as file2:
                file2.write(str(query_idx) + "|" + id_name_dict[key_list[query_idx]] + "\t" + res_name[0] + "|" + "|".join(res_name[1:]) + "\n")
        else:
            with open(vectors_similarity_file, "a", encoding="UTF-8") as file1:
                file1.write(str(query_idx) + "|" + key_list[query_idx] + "\t" + res[0] + "|" + "|".join(res[1:]) + "\n")
            with open(vectors_similarity_name_file, "a", encoding="UTF-8") as file2:
                file2.write(str(query_idx) + "|" + id_name_dict[key_list[query_idx]] + "\t" + res_name[0] + "|" + "|".join(res_name[1:]) + "\n")


id_name_dict = {}
with open("./impala_data/" + str(file_hour_list[-1]) + ".content_basic_2_dict_t_album.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        line = line.strip().split("\t")
        id_name_dict[line[0]] = line[1]
print("len(id_name_dict)", len(id_name_dict))

key_list = []
vector_list = []
content_vector_dict = {}
with open("./model_data/" + str(content_hour_list[-1]) + ".vectors.cbow.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        try:
            line = line.strip().split(" ")
            if len(line) < 3 or line[0] == "</s>":
                continue
            key_list.append(line[0])
            vector_list.append(np.array([float(value) for value in line[1:]], dtype=np.float32))
            content_vector_dict[line[0]] = np.array([float(value) for value in line[1:]], dtype=np.float32)
        except:
            print(line)

# print(key_list[:100])
print(len(key_list))
print(len(vector_list))
print(len(vector_list[1]))
print("len(content_vector_dict)", len(content_vector_dict))

for i in range(len(vector_list)):
    if len(vector_list[i]) != 100:
        print(i, key_list[i], len(vector_list[i]))

vector_array = np.array(vector_list, dtype=np.float32)
print("vector_array.shape", vector_array.shape)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# similarity_v1 = matrix_cosine(vector_array, vector_array)
# print("similarity_v1.shape", similarity_v1.shape)
#
# i2i_save_file(Topk, similarity_v1, key_list, id_name_dict, "./model_data/" + str(content_hour_list[-1]) + ".vectors_similarity.txt", "./model_data/" + str(content_hour_list[-1]) + ".vectors_similarity_name.txt")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def u2i_save_file(Topk, user_content_id_reco_dict, id_name_dict, vectors_similarity_file, vectors_similarity_name_file, user_his_dict):
    print("len(user_content_id_reco_dict)", len(user_content_id_reco_dict))
    with open(vectors_similarity_file, "w", encoding="UTF-8") as file:
        for key, value in user_content_id_reco_dict.items():
            file.write(key + "\t" + "|".join(value) + "\n")

    with open(vectors_similarity_name_file, "w", encoding="UTF-8") as file:
        for key, value in user_content_id_reco_dict.items():
            his_res_name = [id_name_dict[k] for k in user_his_dict[key]]
            res_name = [id_name_dict[k] for k in value]
            file.write(key + "\n" + "|".join(his_res_name) + "\n" + "|".join(res_name) + "\n\n")


user_his_dict = {}
with open("./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        line = line.strip().split("\t")
        if len(line) != 2:
            print(len(line), line)
            continue
        content_id_list = [item.split("|")[1] for item in line[1].split(";") if item.split("|")[1] in content_vector_dict]
        unique_list = list(OrderedDict.fromkeys(content_id_list))
        if len(content_id_list) > 0 and len(content_id_list) == len(unique_list):
            user_his_dict[line[0]] = unique_list
        if len(user_his_dict) >= 100:
            break

for k, v in user_his_dict.items():
    print(k, v)

print("len(user_his_dict)", len(user_his_dict))

content_id_similarity_dict = {}
with open("./model_data/" + str(content_hour_list[-1]) + ".vectors_similarity.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        line = line.strip().split("\t")
        content_id_similarity_dict[line[0].split("|")[1]] = line[1].split(" ")[1:]
print("len(content_id_similarity_dict)", len(content_id_similarity_dict))

user_content_id_reco_dict = {}
for key, value in user_his_dict.items():
    user_content_id_reco_dict.setdefault(key, [])
    for idx in range(Topk - 1):
        for item in value:
            if item in content_id_similarity_dict:
                user_content_id_reco_dict[key].append(content_id_similarity_dict[item][idx])

for k, v in user_content_id_reco_dict.items():
    print(k, len(v), v)

print("len(user_content_id_reco_dict)", len(user_content_id_reco_dict))

u2i_save_file(Topk, user_content_id_reco_dict, id_name_dict, "./model_data/" + str(content_hour_list[-1]) + ".user_vectors_similarity.txt", "./model_data/" + str(content_hour_list[-1]) + ".user_vectors_similarity_name.txt", user_his_dict)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# def u2i_save_file(Topk, similarity_score_list, user_key_list, key_list, id_name_dict, vectors_similarity_file, vectors_similarity_name_file, user_his_dict):
#     print("len(similarity_score_list)", len(similarity_score_list))
#     for query_idx in range(len(user_key_list)):
#         res = similarity_score_list[query_idx].tolist()
#         res_idx = list(map(res.index, heapq.nlargest(Topk, res)))
#         res_value = heapq.nlargest(Topk, res)
#         # res = [str(idx) + "|" + key_list[idx] + "|" + str(value) for idx, value in zip(res_idx, res_value)]
#         res = [key_list[idx] for idx, value in zip(res_idx, res_value) if key_list[idx] in id_name_dict]
#         res_name = [id_name_dict[key] for key in res]
#         his_res_name = [id_name_dict[key] for key in user_his_dict[user_key_list[query_idx]]]
#         if query_idx == 0:
#             with open(vectors_similarity_file, "w", encoding="UTF-8") as file1:
#                 file1.write(str(query_idx) + "|" + user_key_list[query_idx] + "\t" + "|".join(res) + "\n")
#             with open(vectors_similarity_name_file, "w", encoding="UTF-8") as file2:
#                 file2.write(str(query_idx) + "|" + user_key_list[query_idx] + "\n" + "|".join(his_res_name) + "\n" + "|".join(res_name) + "\n\n")
#         else:
#             with open(vectors_similarity_file, "a", encoding="UTF-8") as file1:
#                 file1.write(str(query_idx) + "|" + user_key_list[query_idx] + "\t" + "|".join(res) + "\n")
#             with open(vectors_similarity_name_file, "a", encoding="UTF-8") as file2:
#                 file2.write(str(query_idx) + "|" + user_key_list[query_idx] + "\n" + "|".join(his_res_name) + "\n" + "|".join(res_name) + "\n\n")
#
#
# user_his_dict = {}
# with open("./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence.txt", encoding="UTF-8") as file:
#     for line in file.readlines():
#         line = line.strip().split("\t")
#         if len(line) != 2:
#             print(len(line), line)
#             continue
#         content_id_list = [item.split("|")[1] for item in line[1].split(";") if item.split("|")[1] in content_vector_dict]
#         unique_list = list(OrderedDict.fromkeys(content_id_list))
#         if len(content_id_list) > 0 and len(content_id_list) == len(unique_list):
#             user_his_dict[line[0]] = unique_list
#         # else:
#         #     print(line, content_id_list, unique_list)
#
#         if len(user_his_dict) >= 100:
#             break
#
# print("len(user_his_dict)", len(user_his_dict))
# for k, v in user_his_dict.items():
#     print(k, v)
#
# user_key_list = []
# user_vector_list = []
# for key, value in user_his_dict.items():
#     user_key_list.append(key)
#     tmp_list = [content_vector_dict[item] for item in value]
#     tmp_list_array = np.array(tmp_list, dtype=np.float32)
#     user_vector_list.append(np.mean(tmp_list_array, axis=0))
# print("len(user_key_list)", len(user_key_list))
# print("len(user_vector_list)", len(user_vector_list))
# # print(user_key_list[1])
# # print(user_vector_list[1])
#
# user_vector_array = np.array(user_vector_list, dtype=np.float32)
# print("user_vector_array.shape", user_vector_array.shape)
#
# similarity_v2 = matrix_cosine(user_vector_array, vector_array)
# print("similarity_v2.shape", similarity_v2.shape)
#
# u2i_save_file(Topk, similarity_v2, user_key_list, key_list, id_name_dict, "./model_data/" + str(content_hour_list[-1]) + ".user_vectors_similarity.txt", "./model_data/" + str(content_hour_list[-1]) + ".user_vectors_similarity_name.txt", user_his_dict)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# # zip -r item2vec.zip item2vec -x "*.txt"
#
# # 57 15 * * * source /etc/profile && pyenv global 3.7.9 && source ~/.bashrc && cd /data/azx_reco/yt_recall/item2vec_1 && /home/ubuntu/.pyenv/versions/3.7.9/bin/python3 -u ./item2vec_1.py 1>>./log.txt 2>&1

os.system("ls -la ./impala_data")
os.system("ls -la ./sample_data")
os.system("ls -la ./model_data")
print(str(content_hour_list[-1]) + " DONE!")
