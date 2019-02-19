#!/usr/bin/env python3

import matplotlib.pyplot as plt
import numpy as np

data = dict()

with open("./output_rust/final", "r") as f:
  for line in f:
    line = line.strip()
    year, num = line.split("\t", 1)
    try:
      year = int(year)
      num = int(num)
      data[year] = num
    except ValueError as e:
      print(e)

x = np.arange(len(data))
plt.bar(x, list(data.values()))
plt.xticks(x, list(data.keys()))
plt.show()