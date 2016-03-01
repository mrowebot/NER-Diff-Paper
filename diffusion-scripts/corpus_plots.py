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


def plotEntityTS(k):
    # get the top k entities that were mentioned
    frequencies = {}
    with open('../data/logs/entity_mention_frequencies.csv', 'rb') as csvfile:
        mentions_reader = csv.reader(csvfile, delimiter='\t')
        for row in mentions_reader:
            frequencies[str(row[0])] = int(row[1])
    data = {'counts': pd.Series(frequencies.values(), index=frequencies.keys())}
    df = pd.DataFrame(data)
    df = df.sort('counts', ascending=False)
    # top_entities = df[0:k].index.tolist()
    top_entities = [df.index[i] for i in range(0, k)]

    print top_entities

    # load the time-series data
    entity_ts_string = {}
    csv.field_size_limit(sys.maxsize)
    with open('../data/logs/entity_mention_ts.csv', 'rb') as csvfile:
        mentions_reader = csv.reader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
        for row in mentions_reader:
            entity = str(row[0])
            # print entity
            if entity in top_entities:
                ts_string = ""
                for j in range(1, len(row)-1):
                    ts_string += str(row[j]) + ","
                ts_string = ts_string[:-1]
                entity_ts_string[entity] = ts_string

    # print entity_ts_string.keys()

    # for each user, determine a time-series object and plot this on a graph
    matplotlib.rcParams.update({'font.size': 8})
    plt.figure(1)
    fig_count = 1
    for entity in top_entities:
        # print entity
        ts_string = entity_ts_string[entity]
        ts_objects = ts_string.split(",")
        date_frequencies = {}
        for ts_object in ts_objects:
            date = datetime.strptime(ts_object.split("|")[0], '%Y-%m-%d')
            date_frequencies[date] = int(ts_object.split("|")[1])
        user_ts_date = Series(np.array(date_frequencies.values()), index=date_frequencies.keys())
        plt.subplot(3, 3, fig_count)
        user_ts_date.plot()
        locs, labels = plt.xticks()
        plt.setp(labels, rotation=45)
        plt.xlabel("Date")
        plt.title(entity)

        fig_count += 1

    plt.tight_layout()
    plt.savefig('../plots/entity_ts.pdf')
    plt.clf()
