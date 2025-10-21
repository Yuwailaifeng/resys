#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"
os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
os.environ["CUDA_VISIBLE_DEVICES"] = "7"

import sys
import platform
import numpy as np
import matplotlib.pyplot as plt
import math
import sklearn
from sklearn.model_selection import train_test_split
import tensorflow as tf

# import tensorflow.compat.v1 as tf
# tf.disable_v2_behavior()

np.set_printoptions(suppress=True)
plt.style.use("ggplot")

print(f"Python {sys.version}")
print(f"Python Platform {platform.platform}")
print(f"Numpy {np.__version__}")
print(f"Scikit-Learn {sklearn.__version__}")
print(f"TensorFlow {tf.__version__}")

np.random.seed(10)
tf.set_random_seed(10)

print(tf.test.is_gpu_available())
# tf.config.list_physical_devices('GPU')
# tf.debugging.set_log_device_placement(True)

# 通过占位符定义变量
a = tf.placeholder(tf.int32)
b = tf.placeholder(tf.int32)

# a与b相加
add = tf.add(a, b)
# a与b相乘
mul = tf.multiply(a, b)
# a与b矩阵相乘
mat = tf.matmul(a, b)

gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=0.5)
config = tf.ConfigProto(log_device_placement=True, gpu_options=gpu_options)
config.gpu_options.allow_growth = True
# 启动session
with tf.Session(config=config) as sess:
    # 计算具体数值  使用feed机制
    print("相加: %i" % sess.run(add, feed_dict={
        a: 3,
        b: 4
    }))
    print("相乘: %i" % sess.run(mul, feed_dict={
        a: 3,
        b: 4
    }))

    for i in range(1, 1000000):
        res = sess.run([
            mat,
        ], feed_dict={
            a: np.array(np.arange(i * i * i).reshape([
                i,
                i,
                i,

            ])),
            b: np.array(np.arange(i * i * i).reshape([
                i,
                i,
                i,
            ])).T,
        })
        if i % 10 == 0:
            print(i, len(res[0]))

    # 使用fetch机制
    print(sess.run([
        mul,
        add
    ], feed_dict={
        a: 3,
        b: 4
    }))
