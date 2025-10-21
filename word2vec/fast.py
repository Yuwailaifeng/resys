#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import time
import numpy as np
import fasttext
from gensim.models import Word2Vec
import gensim

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# model = fasttext.train_unsupervised("text8", model="cbow", dim=100, ws=8, epoch=30, lr=0.01, minn=0, maxn=0, thread=10)

# model.save_model("fasttext_model.bin")
model = fasttext.load_model("fasttext_model.bin")

print(model.get_word_vector("china"))
print(model.get_nearest_neighbors("china", k=30))

with open("vectors.fasttext.txt", "w", encoding="UTF-8") as file:
    for word in model.get_words():
        vector = model.get_word_vector(word)
        file.write(str(word) + " ".join(map(str, vector)) + "\n")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# perl wikifil.pl enwik9 > enwik9.txt

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

sentences = gensim.models.word2vec.LineSentence("text8")
model = Word2Vec(sentences, sg=0, vector_size=100, window=8, min_count=5, hs=0, epochs=300, workers=20)

model.save("word2vec.model")

model = Word2Vec.load("word2vec.model")

for key in model.wv.similar_by_word("china", topn=30):
    print(key)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #






