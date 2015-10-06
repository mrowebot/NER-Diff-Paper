#!/usr/bin/env python

from itertools import chain
import nltk
#from sklearn.preprocessing import LabelBinarizer
#import sklearn
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

# takes a list of token/label pairs; returns a list of [feature]/label pairs
def featurise(sentence):
	for i in range(len(sentence)):
		features = []
		if sentence[i][0] in brown_cluster:
			for j in range(1,len(brown_cluster[sentence[i][0]])+1):
				features.append('path' + str(j) + 'b' + brown_cluster[sentence[i][0]][0:j])
		yield (features, sentence[i][1])


input = [['oh', 'O'], ['god', 'PER'], ['not', 'O'], ['again', 'O']]

for f in featurise(input):
	print f