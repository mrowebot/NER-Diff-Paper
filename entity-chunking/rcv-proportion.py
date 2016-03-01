#!/usr/bin/env python3

import json
import csv
from pandas import Series
import numpy as np
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import sys

rcv_blend = [0.5646,0.6204,0.6264,0.6414,0.6138,0.6014,0.5972,0.5986,0.5813,0.5942]

rcv_twitter_blend = [0.4717,0.5303,0.5400,0.5447,0.5616,0.5522,0.5338,0.5201,0.5508,0.5224]

rcv_rcv = [0.6134,0.6110,0.6083,0.6108,0.6082,0.5918,0.5973,0.5959,0.6055,0.5959]



matplotlib.rcParams.update({'font.size': 8})
plt.setp(labels, rotation=45)
plt.xlabel("Proportion RCV used")
plt.title(entity)

fig_count += 1
plt.plot(rcv_blend)

plt.tight_layout()
plt.savefig('../entity_chunking/rcvtrainingdata.pdf')
plt.clf()
