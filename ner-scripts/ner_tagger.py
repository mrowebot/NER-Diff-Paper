#!/usr/bin/env python

from itertools import chain
import nltk
from sklearn.preprocessing import LabelBinarizer
import sklearn
import pycrfsuite
import sys


brown_cluster = {}
for line in open(sys.argv[1], 'r'):
	line = line.strip()
	if not line:
		continue
	path,token = line.split()[0:2]
	brown_cluster[token] = path

# X : list of lists of instances, each instance is a list of feature reprs
# y : list of lists of labels