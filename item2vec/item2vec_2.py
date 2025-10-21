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

import math
from multiprocessing import Pool
import heapq
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

pool_num = 10
print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

impala_hour = 1
file_hour_list = []
for idx in range(impala_hour):
    file_hour = (datetime.datetime.now() + datetime.timedelta(days=idx - impala_hour)).date().strftime("%Y%m%d")
    file_hour_list.append(file_hour)
print(impala_hour, file_hour_list)

content_hour = 30
content_hour_list = []
for idx in range(content_hour):
    file_hour = (datetime.datetime.now() + datetime.timedelta(days=idx - content_hour)).date().strftime("%Y%m%d")
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
        WHERE `ds` = '""" + str(file_hour) + """' AND `content_type` = 2 AND `device_uuid` IS NOT NULL AND `content_id_c` IS NOT NULL 
        GROUP BY `device_uuid`
    """)

try:
    with connection.cursor() as cursor:
        # # sql = """SELECT CONCAT(CAST(`content_type` AS STRING), "|", `content_type_name`)  content, COUNT(*) content_count FROM `dw_v2`.`user_ls_day_summary` WHERE `ds` = '20251009' GROUP BY `content` ORDER BY `content_count` DESC"""
        # # sql = """SELECT * FROM `dm`.`recommend_waterfall_v2_content_list` WHERE `ds` = '""" + str(file_hour_list[-1]) + """' AND `content_type` = '专辑'"""
        # # sql = """SELECT DISTINCT `content_id_c`, `content_name_f` FROM `dw_v2`.`user_ls_day_summary` WHERE `ds` = '20251013' AND `content_type` = 1"""
        # # sql = """SELECT `id`, `broadcast_name`, `listen_count` FROM `dict`.`content_basic_2_dict_t_broadcast` WHERE `listen_count` >= 0"""
        # sql = """SELECT * FROM `dict`.`content_basic_2_dict_t_broadcast` WHERE `enable_status` = 1 AND `delete_flag` = 0"""
        # print(sql)
        #
        # cursor.execute(sql)
        # result = cursor.fetchall()
        # columns = [item[0] for item in cursor.description]
        # tempDf = pd.DataFrame(result, columns=columns)
        #
        # print("`dict`.`content_basic_2_dict_t_broadcast`", file_hour_list[-1], len(tempDf), columns)
        # tempDf.to_csv("./impala_data/" + str(file_hour_list[-1]) + ".content_basic_2_dict_t_broadcast.txt", header=columns, index=False, sep="\t", encoding="UTF-8")

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

        # t1.content_id, t1.page_name, t2.content_name_f, t2.content_id_c, t1.content_type, t1.channel_id


        sql = """
            SELECT DISTINCT t1.content_id, t1.page_name, t2.content_name, t2.content_id_f, t2.content_name_f, t1.content_type, t1.channel_id
            FROM dm.recommend_waterfall_v2_content_list t1
            LEFT JOIN dw_v2.user_ls_day_summary t2
            ON t1.content_id = t2.content_id_c
            WHERE t1.ds = '""" + str(file_hour_list[-1]) + """' AND t1.content_type = '电台节目'
                AND t2.ds = '""" + str(file_hour_list[-1]) + """' AND t2.content_type = 2 AND t2.content_name IS NOT NULL AND t2.content_name_f IS NOT NULL 
        """
        print(sql)

        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [item[0] for item in cursor.description]
        tempDf = pd.DataFrame(result, columns=columns)

        print("content_broadcast_for_recs", file_hour_list[-1], len(tempDf), columns)
        tempDf.to_csv("./model_data/" + str(file_hour_list[-1]) + ".content_broadcast_for_recs.txt", header=columns, index=False, sep="\t", encoding="UTF-8")

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
with open("./model_data/" + str(file_hour_list[-1]) + ".content_broadcast_for_recs.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        line = line.strip().split("\t")
        page_name_dict.setdefault(line[0], [])
        channel_id_dict.setdefault(line[0], [])
        if line[1] not in page_name_dict[line[0]]:
            page_name_dict[line[0]].append(line[1])
            channel_id_dict[line[0]].append(line[-1])
        id_name_dict[line[0]] = line[3] + "|" + "#".join(page_name_dict[line[0]]) + "|" + line[2] + "|" + line[4] + "|" + "#".join(channel_id_dict[line[0]])
print("len(id_name_dict)", len(id_name_dict))

with open("./model_data/" + str(content_hour_list[-1]) + ".id_name_dict.txt", "w", encoding="UTF-8") as file:
    for key, value in id_name_dict.items():
        file.write(key + "\t" + value + "\n")

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

with open("./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence_for_rec_name.txt", "w", encoding="UTF-8") as file:
    for device_uuid, content_list in user_content_sequence_dict.items():
        if len(content_list) > 1:
            unique_list = list(OrderedDict.fromkeys(content_list))
            unique_list = [id_name_dict[item].split("|")[2] for item in unique_list]
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
os.system("time ./word2vec -train ./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence_for_rec_name.txt -output ./model_data/" + str(content_hour_list[-1]) + ".vectors.cbow.binary -cbow 1 -size 100 -window 8 -negative 25 -hs 0 -sample 1e-4 -threads 10 -binary 1 -iter 30")
os.system("ls -la ./model_data")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def matrix_cosine(matrix1, matrix2):
    return cosine_similarity(matrix1, matrix2)


def i2i_idx_name(similarity_score_list, key_list, id_name_dict, process_idx, start_idx, batch_size):
    # top_k = min(3000, len(similarity_score_list))
    top_k = len(similarity_score_list)
    # print("len(similarity_score_list)", len(similarity_score_list))
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(process_idx, "i2i_idx_name start", start_time, start_idx, start_idx + batch_size)
    res_idx_list = []
    res_name_list = []
    for query_idx in range(start_idx, start_idx + batch_size):
        if query_idx >= len(similarity_score_list):
            break
        similarity_idx = list(map(similarity_score_list[query_idx].index, heapq.nlargest(top_k, similarity_score_list[query_idx])))
        # similarity_value = heapq.nlargest(top_k, similarity_score_list[query_idx])
        res_idx = [key_list[idx] + "|" + id_name_dict[key_list[idx]].split("|")[-1] for idx in similarity_idx]
        res_name = [key_list[idx] + "|" + id_name_dict[key_list[idx]] for idx in similarity_idx]
        res_idx_list.append(key_list[query_idx] + "|" + id_name_dict[key_list[query_idx]].split("|")[-1] + "\t" + ";".join(res_idx[1:]))
        res_name_list.append(key_list[query_idx] + "|" + id_name_dict[key_list[query_idx]] + "\t" + " ; ".join(res_name[1:]))
    done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(process_idx, "i2i_idx_name done", done_time, start_idx, start_idx + batch_size)
    return res_idx_list, res_name_list


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

pool = Pool(pool_num)
batch_size = 1000
process_num = math.ceil(len(similarity_score_list) / batch_size)
print("process_num", process_num, len(similarity_score_list), batch_size)
results = []
for process_idx in range(process_num):
    # batch_size = math.ceil(len(user_his_seq_list) / 100)
    start_idx = process_idx * batch_size
    async_results = pool.apply_async(i2i_idx_name, args=(
        similarity_score_list,
        key_list,
        id_name_dict,
        process_idx,
        start_idx,
        batch_size,
    ))
    results.append(async_results)
# p.map(long_time_task, [i for i in range(5)])
print('Waiting for all subprocesses done...')
pool.close()
pool.join()

# output_0 = [item.get()[0] for item in results]
# print(output_0)

res_idx_list = []
res_name_list = []
for item in results:
    res_idx_list.extend(item.get()[0])
    res_name_list.extend(item.get()[1])
# for i in range(10):
#     print(i, res_idx_list[i])
# print(len(res_idx_list))

# output_1 = [item.get()[1] for item in results]
# print(output_1)

# print(len(output_0))
# print(len(output_1))


with open("./model_data/" + str(content_hour_list[-1]) + ".vectors_similarity.txt", "w", encoding="UTF-8") as file:
    for line in res_idx_list:
        file.write(line + "\n")

with open("./model_data/" + str(content_hour_list[-1]) + ".vectors_similarity_name.txt", "w", encoding="UTF-8") as file:
    for line in res_name_list:
        file.write(line + "\n")

# with open("./model_data/vectors_similarity.txt", "w", encoding="UTF-8") as file:
#     for line in res_idx_list:
#         file.write(line + "\n")
#
# with open("./model_data/vectors_similarity_name.txt", "w", encoding="UTF-8") as file:
#     for line in res_name_list:
#         file.write(line + "\n")

print("len(res_idx_list)", len(res_idx_list))
print("len(res_name_list)", len(res_name_list))


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def u2i_idx_name(user_his_seq_list, content_id_similarity_dict, id_name_dict, process_idx, start_idx, batch_size):
    # top_k = min(3000, len(user_his_seq_list))
    top_k = len(user_his_seq_list)
    # print("len(user_his_seq_list)", len(user_his_seq_list))
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(process_idx, "u2i_idx_name start", start_time, start_idx, start_idx + batch_size)
    res_idx_dict = {}
    res_his_dict = {}
    res_name_dict = {}
    for query_idx in range(start_idx, start_idx + batch_size):
        if query_idx >= len(user_his_seq_list):
            break
        # if query_idx != start_idx and query_idx % 10000 == 0:
        #     print(process_idx, query_idx, start_idx, start_idx + batch_size, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        device_uuid, user_his_seq = user_his_seq_list[query_idx]
        content_id_list = [item for item in user_his_seq.split(";") if item in content_id_similarity_dict]
        unique_list = list(OrderedDict.fromkeys(content_id_list))
        if len(content_id_list) <= 0 or len(content_id_list) != len(unique_list):
            # print(query_idx, user_his_seq_list[query_idx], len(content_id_list), len(content_id_list), len(unique_list), content_id_list, unique_list)
            continue
        res_his_dict[device_uuid] = unique_list

        for idx in range(min(len(content_id_similarity_dict) - 1, 2000)):
            for item in unique_list:
                if item in content_id_similarity_dict and content_id_similarity_dict[item][idx] not in unique_list:
                    tmp_item = content_id_similarity_dict[item][idx]
                    for page_name, channel_id in zip(id_name_dict[tmp_item].split("|")[1].split("#"), id_name_dict[tmp_item].split("|")[-1].split("#")):
                        dict_key = device_uuid + "_" + channel_id + "|" + page_name
                        res_idx_dict.setdefault(dict_key, [])
                        res_name_dict.setdefault(device_uuid, [])
                        if len(res_idx_dict[dict_key]) <= 300 and tmp_item not in res_idx_dict[dict_key]:
                            res_idx_dict[dict_key].append(tmp_item)
                        if len(res_name_dict[device_uuid]) <= 100 and tmp_item not in res_name_dict[device_uuid]:
                            res_name_dict[device_uuid].append(tmp_item)

    with open("./tmp_data/" + str(content_hour_list[-1]) + "." + str(process_idx) + ".user_content_reco.txt", "w", encoding="UTF-8") as file:
        for key, value in res_idx_dict.items():
            # tmp_idx_res = [k + "|" + id_name_dict[k].split("|")[-1] for k in value]
            file.write(key + "\t" + ";".join(value) + "\n")

    with open("./tmp_data/" + str(content_hour_list[-1]) + "." + str(process_idx) + ".user_content_reco_name.txt", "w", encoding="UTF-8") as file:
        for key, value in res_name_dict.items():
            tmp_his_res = [k + "|" + id_name_dict[k] for k in res_his_dict[key]]
            tmp_idx_res = [k + "|" + id_name_dict[k] for k in value]
            file.write(key + "\n" + " ; ".join(tmp_his_res) + "\n" + " ; ".join(tmp_idx_res) + "\n\n")

    done_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(process_idx, "u2i_idx_name done ", done_time, start_idx, start_idx + batch_size, len(res_idx_dict), len(res_name_dict), len(res_his_dict))

    return len(res_idx_dict), len(res_name_dict)


content_id_similarity_dict = {}
with open("./model_data/" + str(content_hour_list[-1]) + ".vectors_similarity.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        line = line.strip().split("\t")
        content_id_similarity_dict[line[0].split("|")[0]] = [item.split("|")[0] for item in line[1].split(";")]
print("len(content_id_similarity_dict)", len(content_id_similarity_dict))

user_his_seq_list = []
with open("./sample_data/" + str(content_hour_list[-1]) + ".device_uuid_content_id_sequence.txt", encoding="UTF-8") as file:
    for line in file.readlines():
        line = line.strip().split("\t")
        if len(line) != 2:
            print(len(line), line)
            continue
        user_his_seq_list.append(line)
print("len(user_his_seq_list)", len(user_his_seq_list))
user_his_seq_list = user_his_seq_list

pool = Pool(pool_num * 2)
batch_size = 60000
process_num = math.ceil(len(user_his_seq_list) / batch_size)
print("process_num", process_num, len(user_his_seq_list), batch_size)
results = []
for process_idx in range(process_num):
    # batch_size = math.ceil(len(user_his_seq_list) / 100)
    start_idx = process_idx * batch_size
    async_results = pool.apply_async(u2i_idx_name, args=(
        user_his_seq_list,
        content_id_similarity_dict,
        id_name_dict,
        process_idx,
        start_idx,
        batch_size,
    ))
    results.append(async_results)
# p.map(long_time_task, [i for i in range(5)])
print('Waiting for all subprocesses done...')
pool.close()
pool.join()

res_idx = []
res_name = []
for item in results:
    res_idx.append(item.get()[0])
    res_name.append(item.get()[1])

res_idx_list = []
res_name_list = []
for process_idx in range(process_num):
    with open("./tmp_data/" + str(content_hour_list[-1]) + "." + str(process_idx) + ".user_content_reco.txt", encoding="UTF-8") as file:
        for line in file.readlines():
            res_idx_list.append(line)
    print(process_idx, "len(res_idx_list)", len(res_idx_list))

    with open("./tmp_data/" + str(content_hour_list[-1]) + "." + str(process_idx) + ".user_content_reco_name.txt", encoding="UTF-8") as file:
        for line in file.readlines():
            res_name_list.append(line)
    print(process_idx, "len(res_name_list)", len(res_name_list))

print("len(res_idx_list)", len(res_idx_list))
print("len(res_name_list)", len(res_name_list))

with open("./model_data/" + str(content_hour_list[-1]) + ".user_content_reco.txt", "w", encoding="UTF-8") as file:
    for line in res_idx_list:
        file.write(line)

with open("./model_data/" + str(content_hour_list[-1]) + ".user_content_reco_name.txt", "w", encoding="UTF-8") as file:
    for line in res_name_list:
        file.write(line)

print("res_idx", res_idx)
print("res_name", res_name)
print("len(res_idx_list)", len(res_idx_list))
print("len(res_name_list)", len(res_name_list))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

os.system("head -n 1000 " + "./model_data/" + str(content_hour_list[-1]) + ".user_content_reco_name.txt" + " > ./model_data/user_content_reco_name.txt")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# # zip -r item2vec.zip item2vec -x "*.txt"
#
# 02  09 * * * source /etc/profile && pyenv global 3.7.9 && source ~/.bashrc && cd /data/azx_reco/yt_recall/item2vec_2 && /home/ubuntu/.pyenv/versions/3.7.9/bin/python3 -u ./item2vec_2.py 1>>./log.txt 2>&1

os.system("ls -lah ./impala_data")
os.system("ls -lah ./sample_data")
os.system("ls -lah ./model_data")

hdfs_file_path = "/user/admin/an_zhong_xin/yt_recall/item2vec_2/" + str(content_hour_list[-1] + "/")
print("hdfs_file_path", hdfs_file_path)

print("hadoop fs -mkdir " + hdfs_file_path)
os.system("hadoop fs -mkdir " + hdfs_file_path)

print("hadoop fs -put -f ./model_data/" + str(content_hour_list[-1]) + "* " + hdfs_file_path)
os.system("hadoop fs -put -f ./model_data/" + str(content_hour_list[-1]) + "* " + hdfs_file_path)

print("hadoop fs -ls " + hdfs_file_path)
os.system("hadoop fs -ls -h " + hdfs_file_path)

print("rm -rf ./tmp_data/*")
os.system("rm -rf ./tmp_data/*")

pre_day = (datetime.datetime.now() + datetime.timedelta(days=-5)).date().strftime("%Y%m%d")
print("rm -rf ./model_data/" + str(pre_day) + ".vectors_similarity.txt")
os.system("rm -rf ./model_data/" + str(pre_day) + ".vectors_similarity.txt")
print("rm -rf ./model_data/" + str(pre_day) + ".vectors_similarity_name.txt")
os.system("rm -rf ./model_data/" + str(pre_day) + ".vectors_similarity_name.txt")
print("rm -rf ./model_data/" + str(pre_day) + ".user_content_reco.txt")
os.system("rm -rf ./model_data/" + str(pre_day) + ".user_content_reco.txt")
print("rm -rf ./model_data/" + str(pre_day) + ".user_content_reco_name.txt")
os.system("rm -rf ./model_data/" + str(pre_day) + ".user_content_reco_name.txt")

print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print(str(content_hour_list[-1]) + " DONE!")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
