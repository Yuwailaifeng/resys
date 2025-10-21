#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# import time
# import numpy as np

# start = time.time()

# vec1 = np.array([1, 2, 3, 4])
# vec2 = np.array([5, 6, 7, 8])

# cos_sim = vec1.dot(vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
# print(cos_sim)

# print("Total time %s" % (time.time() - start))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# import time
# import numpy as np
# from sklearn.metrics.pairwise import cosine_similarity

# start = time.time()

# vec1 = np.array([1, 2, 3, 4])
# vec2 = np.array([5, 6, 7, 8])

# cos_sim = cosine_similarity(vec1.reshape(1, -1), vec2.reshape(1, -1))
# print(cos_sim[0][0])

# print("Total time %s" % (time.time() - start))

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
import heapq
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

Topk = 101


def cosine_v1(vec1, vec2):
    return vec1.dot(vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))


def cosine_v2(vec1, vec2):
    return cosine_similarity(vec1.reshape(1, -1), vec2.reshape(1, -1))


def matrix_cosine_v1(matrix1, matrix2):
    return cosine_similarity(matrix1, matrix2)


def matrix_cosine_v2(matrix1, matrix2):
    return np.dot(matrix1, matrix2.T) / (
                np.linalg.norm(matrix1, axis=1).reshape(1, -1) * np.linalg.norm(matrix2, axis=1).reshape(-1, 1))


def matrix_print(query_idx, similarity_score_list):
    res = similarity_score_list[query_idx].tolist()
    res_idx = list(map(res.index, heapq.nlargest(Topk, res)))
    res_value = heapq.nlargest(Topk, res)
    print("query_idx", query_idx, key_list[query_idx])
    print("res_idx", res_idx)
    print("res_value", res_value)
    for idx, value in zip(res_idx, res_value):
        print(idx, key_list[idx], value)
    print("res_list: ", " ".join([key_list[idx] for idx in res_idx]))


# def matrix_save_file(Topk, similarity_score_list, key_list):
#     print("len(similarity_score_list)", len(similarity_score_list))
#     for query_idx in range(len(similarity_score_list)):
#         res = similarity_score_list[query_idx].tolist()
#         res_idx = list(map(res.index, heapq.nlargest(Topk, res)))
#         res_value = heapq.nlargest(Topk, res)
#         # print("query_idx", query_idx, key_list[query_idx])
#         # print("res_idx", res_idx)
#         # print("res_value", res_value)
#         # for idx, value in zip(res_idx, res_value):
#         #     print(idx, key_list[idx], value)
#         # print("res_list: ", " ".join([key_list[idx] for idx in res_idx]))
#         if query_idx == 0:
#             with open("vectors_file.txt", "w", encoding="UTF-8") as file:
#                 res = [str(idx)+"|"+key_list[idx]+"|"+str(value) for idx, value in zip(res_idx, res_value)]
#                 file.write(res[0]+" "+" ".join(res[1:]) + "\n")
#         else:
#             with open("vectors_file.txt", "a", encoding="UTF-8") as file:
#                 res = [str(idx)+"|"+key_list[idx]+"|"+str(value) for idx, value in zip(res_idx, res_value)]
#                 file.write(res[0]+" "+" ".join(res[1:]) + "\n")

def matrix_save_file(Topk, similarity_score_list, key_list):
    print("len(similarity_score_list)", len(similarity_score_list))
    for query_idx in range(10):
        res = similarity_score_list[query_idx].tolist()
        res_idx = list(map(res.index, heapq.nlargest(Topk, res)))
        res_value = heapq.nlargest(Topk, res)
        # print("query_idx", query_idx, key_list[query_idx])
        # print("res_idx", res_idx)
        # print("res_value", res_value)
        # for idx, value in zip(res_idx, res_value):
        #     print(idx, key_list[idx], value)
        # print("res_list: ", " ".join([key_list[idx] for idx in res_idx]))
        if query_idx == 0:
            with open("vectors_file.txt", "w", encoding="UTF-8") as file:
                res = [str(idx) + "|" + key_list[idx] + "|" + str(value) for idx, value in zip(res_idx, res_value)]
                print(query_idx, res[0] + " " + " ".join(res[1:]) + "\n")
        else:
            with open("vectors_file.txt", "a", encoding="UTF-8") as file:
                res = [str(idx) + "|" + key_list[idx] + "|" + str(value) for idx, value in zip(res_idx, res_value)]
                print(query_idx, res[0] + " " + " ".join(res[1:]) + "\n")


if __name__ == "__main__":

    # vec1 = np.array([1, 2, 3, 4])
    # vec2 = np.array([5, 6, 7, 8])

    # start = time.time()
    # cos_sim = cosine_v1(vec1, vec2)
    # print(cos_sim)
    # print("Total time %s" % (time.time() - start))

    # start = time.time()
    # cos_sim = cosine_v2(vec1, vec2)
    # print(cos_sim[0][0])
    # print("Total time %s" % (time.time() - start))

    key_list = []
    vector_list = []
    with open("vectors.skipgram.txt", encoding="UTF-8") as file:
        for line in file.readlines():
            line = line.strip().split(" ")
            if len(line) < 3:
                continue
            key_list.append(line[0])
            vector_list.append(np.array([float(value) for value in line[1:]], dtype=np.float32))

    print(len(key_list))
    print(len(vector_list))
    print(len(vector_list[1]))

    # for i in range(10):
    #     print(i)
    #     print(key_list[i])
    #     print(vector_list[i])

    vector_array = np.array(vector_list, dtype=np.float)

    similarity_v1 = matrix_cosine_v1(vector_array[:2000], vector_array[:2000])
    print(vector_array.shape)
    print(similarity_v1.shape)
    # print(similarity_v1)

    # similarity_v2 = matrix_cosine_v2(vector_array, vector_array)
    # print(vector_array.shape)
    # print(similarity_v2.shape)
    # # print(similarity_v2)

    # res1 = similarity_v1[486].tolist()
    # res_idx = list(map(res1.index, heapq.nlargest(Topk, res1)))
    # res_value = heapq.nlargest(Topk, res1)
    # print("res_idx", res_idx)
    # print("res_value", res_value)

    # res2 = similarity_v2[486].tolist()
    # res_idx = list(map(res2.index, heapq.nlargest(Topk, res2)))
    # res_value = heapq.nlargest(Topk, res2)
    # print("res_idx", res_idx)
    # print("res_value", res_value)

    # # print(key_list.index("china"))
    # # print(key_list[486])
    # # print([key_list[idx] for idx in res_idx])

    # for idx, value in zip(res_idx, res_value):
    #     print(idx, key_list[idx], value)

    # query_list = ["china", "chinese", "usa", "america"]
    # for query in query_list:
    #     query_idx = key_list.index(query)
    #     matrix_print(query_idx, similarity_v1)


    matrix_save_file(Topk, similarity_v1, key_list)
