#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors: zhongxinan

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import random

test = [i for i in range(100)]
test1 = test[:10]
random.shuffle(test1)
test1.extend(test[10:])
test = test1
print(test1)
print(test)
