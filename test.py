#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "0"
os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
os.environ["CUDA_VISIBLE_DEVICES"] = "7"
import sys
import platform
import numpy as np
import matplotlib.pyplot as plt
import math
import sklearn
from sklearn.model_selection import train_test_split
# import tensorflow as tf
import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()

np.set_printoptions(suppress=True)
plt.style.use("ggplot")

print(f"Python {sys.version}")
print(f"Python Platform {platform.platform}")
print(f"Numpy {np.__version__}")
print(f"Scikit-Learn {sklearn.__version__}")
print(f"TensorFlow {tf.__version__}")

np.random.seed(10)
# tf.set_random_seed(10)

print(tf.test.is_gpu_available())
tf.config.list_physical_devices('GPU')
tf.debugging.set_log_device_placement(True)

# # gpu_options=tf.GPUOptions(per_process_gpu_memory_fraction=0.7)
# # config = tf.ConfigProto(log_device_placement=False, gpu_options=gpu_options)
# # config.gpu_options.per_process_gpu_memory_fraction = 0.7
# # keras.backend.tensorflow_backend.set_session(tf.Session(config=config))
# # session = tf.Session(config=config)
# # sudo pip3 install tensorflow keras numpy scikit-learn scipy matplotlib


# # X = np.linspace(0, 1, 101)[:, np.newaxis]
# # # Y = np.square(X)
# # Y = np.multiply(np.square(X), 10) + 1


X = np.linspace(0, 10, 101)[:, np.newaxis]
Y = np.multiply(np.square(X), 0.1) + 0.1

np.random.seed(10)
np.random.shuffle(X)
np.random.seed(10)
np.random.shuffle(Y)

X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.1, random_state=10)
n = int(X.shape[0] * 0.9)
X_train, X_test, Y_train, Y_test = X[:n], X[n:], Y[:n], Y[n:]

X_train_idx = np.squeeze(np.argsort(X_train[:X_test.shape[0]], axis=0), axis=1)
print("X_train_idx", X_train_idx)
X_train_data, Y_train_data = X_train[X_train_idx], Y_train[X_train_idx]

X_test_idx = np.squeeze(np.argsort(X_test, axis=0), axis=1)
print("X_test_idx", X_test_idx)
X_test, Y_test = X_test[X_test_idx], Y_test[X_test_idx]

print("X_train.shape", X_train.shape)
print("Y_train.shape", Y_train.shape)
print("X_test.shape", X_test.shape)
print("Y_test.shape", Y_test.shape)

for i in range(min(X_train_data.shape[0], 10)):
    print("TRAIN_DATA:", i, X_train_data[i], Y_train_data[i])

for i in range(min(X_test.shape[0], 10)):
    print("TEST_DATA:", i, X_test[i], Y_test[i])

# 创建模型
input = {
    "X": tf.placeholder("float", [None, 1]),
    "Y": tf.placeholder("float", [None, 1]),
}


def layer_norm(input_tensor, name=None):
    """Run layer normalization on the last dimension of the tensor."""
    layer_norma = tf.keras.layers.LayerNormalization(axis=-1)
    return layer_norma(input_tensor)


def swish(x):
    return x / tf.nn.sigmoid(x)


def variable_summaries(var, idx, type):
    with tf.name_scope("summary_" + str(idx) + "_" + type):

        mean = tf.reduce_mean(var)
        tf.summary.scalar("mean", mean)

        with tf.name_scope("stddev"):
            stddev = tf.sqrt(tf.reduce_mean(tf.square(var - mean)))
        tf.summary.scalar("stddev", stddev)
        tf.summary.scalar("max", tf.reduce_max(var))
        tf.summary.scalar("min", tf.reduce_min(var))
        tf.summary.histogram("histogram", var)


# 正向函数
dnn_dims = [200, 200, 1]
net = input["X"]
# net = tf.layers.batch_normalization(inputs=input["X"], name="concat_bn", reuse=tf.AUTO_REUSE)
print(net.get_shape().as_list())

# # net = tf.contrib.layers.layer_norm(inputs=input["X"], scope="concat_ln", reuse=tf.AUTO_REUSE)
# # net = layer_norm(input["X"])
# for idx, units in enumerate(dnn_dims):
#     if idx != len(dnn_dims) - 1:
#         net = tf.layers.dense(net, units=units, activation=tf.nn.relu, name="dnn_" + str(idx))
#         print("net", idx, units, net)
#     else:
#         pred = tf.layers.dense(net, units=units, activation=None, name="dnn_" + str(idx))
#         print("net", idx, units, pred)


w_list = []
b_list = []
for idx in range(len(dnn_dims)):
    if idx == 0:
        w_list.append(tf.Variable(tf.random_normal([net.get_shape().as_list()[1], dnn_dims[idx]], seed=1), name="weight_" + str(idx)))
        b_list.append(tf.Variable(tf.zeros([dnn_dims[idx]]), name="bias_" + str(idx)))
    else:
        w_list.append(tf.Variable(tf.random_normal([dnn_dims[idx-1], dnn_dims[idx]], seed=1), name="weight_" + str(idx)))
        b_list.append(tf.Variable(tf.zeros([dnn_dims[idx]]), name="bias_" + str(idx)))
    variable_summaries(w_list[idx], idx, "w")
    variable_summaries(b_list[idx], idx, "b")
print(w_list)


for idx in range(len(dnn_dims)):
    if idx != len(dnn_dims) - 1:
        net = tf.nn.relu(tf.nn.bias_add(tf.matmul(net, w_list[idx]), b_list[idx]))
        print("net", idx, net)
    else:
        pred = tf.nn.bias_add(tf.matmul(net, w_list[idx]), b_list[idx])
        print("net", idx, pred)


# 反向函数
# loss = tf.reduce_mean(tf.square(input["Y"] - pred))
train_loss_op = tf.losses.mean_squared_error(labels=input["Y"], predictions=pred, weights=1.0)
test_loss_op = tf.losses.mean_squared_error(labels=input["Y"], predictions=pred, weights=1.0)
tf.summary.scalar("train_loss", train_loss_op)
tf.summary.scalar("test_loss", test_loss_op)
# accuracy=tf.reduce_mean(tf.cast(tf.equal(tf.argmax(input["Y"], 1), tf.argmax(y_pred, 1)), tf.float32))

learning_rate = 0.01
# optimizer = tf.train.GradientDescentOptimizer(learning_rate).minimize(loss)
optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate, beta1=0.9, beta2=0.999, epsilon=1e-8).minimize(train_loss_op)
# optimizer = tf.train.AdagradOptimizer(learning_rate=learning_rate, initial_accumulator_value=1e-8).minimize(loss)
# optimizer = tf.train.MomentumOptimizer(learning_rate=learning_rate, momentum=0.95).minimize(loss)
# optimizer = tf.train.FtrlOptimizer(learning_rate).minimize(loss)


import datetime

current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(current_time)
current_time = datetime.datetime.now().strftime("%Y-%m-%d %H%M%S")
print(current_time)

os.system("rm -rf ./logs")
log_dir = "./logs/"
print("\n", "current_time", current_time, log_dir, "\n")

# train_log_dir = "./logs/" + current_time + "/train"
# test_log_dir = "./logs/" + current_time + "/test"
# train_summary_writer = tf.summary.create_file_writer(train_log_dir)
# test_summary_writer = tf.summary.create_file_writer(test_log_dir)


init = tf.global_variables_initializer()
init_local = tf.local_variables_initializer()
merged = tf.summary.merge_all()
plotdata = {"epoch": [], "train_loss": [], "test_loss": []}

# 启动session
with tf.Session() as sess:
    sess.run(init)
    sess.run(init_local)
    writer = tf.summary.FileWriter(log_dir, sess.graph)

    for epoch in range(100000 + 1):
        sess.run(optimizer, feed_dict={input["X"]: X_train, input["Y"]: Y_train})

        if epoch % 100 == 0:
            train_summary, train_loss = sess.run([merged, train_loss_op], feed_dict={input["X"]: X_train, input["Y"]: Y_train})
            test_summary, test_loss = sess.run([merged, test_loss_op], feed_dict={input["X"]: X_test, input["Y"]: Y_test})
            writer.add_summary(train_summary, epoch)
            writer.add_summary(test_summary, epoch)

            print("epoch =", epoch, "train_loss =", train_loss, "test_loss =", test_loss)

            # with train_summary_writer.as_default():
            #     tf.summary.scalar("loss", train_loss, step=epoch)
            # with test_summary_writer.as_default():
            #     tf.summary.scalar("loss", test_loss, step=epoch)
            # tensorboard --logdir /Users/anzhongxin/Desktop/resys/logs
            # http://anzhongxindeMacBook-Pro.local:6006/
            # http://localhost:6006/
            # tensorboard --logdir /home/ubuntu/azx/logs
            # http://localhost:6006/
            # http://10.129.23.11:6006/
            # tensorboard --bind_all --logdir=/home/ubuntu/azx/logs  --port 8080


            if not (train_loss == "NA" or test_loss == "NA"):
                plotdata["epoch"].append(epoch)
                plotdata["train_loss"].append(train_loss)
                plotdata["test_loss"].append(test_loss)

    # 图像显示
    plt.subplot(221)
    plt.plot(X_train_data, Y_train_data, "ro")
    plt.plot(X_train_data, sess.run(pred, feed_dict={input["X"]: X_train_data}))

    plt.subplot(222)
    plt.plot(plotdata["epoch"], plotdata["train_loss"], "b--")

    plt.subplot(223)
    plt.plot(X_test, Y_test, "ro")
    plt.plot(X_test, sess.run(pred, feed_dict={input["X"]: X_test}))

    plt.subplot(224)
    plt.plot(plotdata["epoch"], plotdata["test_loss"], "b--")

    plt.savefig("test.png", dpi=300)
    # plt.show()

    pred_val = sess.run(pred, feed_dict={input["X"]: X_test, input["Y"]: Y_test})
    for i in range(min(X_test.shape[0], 10)):
        print("TEST_DATA:", i, X_test[i], Y_test[i], pred_val[i])

