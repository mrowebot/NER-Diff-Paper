__author__ = 'rowem'
import json
import csv
from pandas import Series
import numpy as np
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import sys

# plot the k most common entities that are found within the db
def plotPerEntityDistribution(k):
    frequencies = {}
    # Read in the mention frequencies
    with open('../data/logs/entity_mention_frequencies.csv', 'rb') as csvfile:
        mentions_reader = csv.reader(csvfile, delimiter='\t')
        for row in mentions_reader:
            try:
                frequencies[str(row[0])] = int(row[1])
            except Exception as e:
                print e.message

    data = {'counts': pd.Series(frequencies.values(), index=frequencies.keys())}
    df = pd.DataFrame(data)
    df = df.sort('counts', ascending=False)
    df[0:(k-1)].plot(kind='bar', legend=False)
    plt.savefig('../plots/entity_freq_top_k_' + str(k) + '.pdf', bbox_inches='tight')
    plt.clf()

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


def getTopKEntities(k):
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

    # write to local file the top k entities
    output_string = ""
    for entity in top_entities:
        output_string += str(entity) + "\n"
    f = open('../data/logs/top' + str(k) + "_entities.csv",'w')
    f.write(output_string) # python will convert \n to os.linesep
    f.close() # you can omit in most cases as the destructor will call it


### Main execution code
# ks = [20, 40, 60, 80, 100]
# ks = [9]
# for k in ks:
# #     plotPerEntityDistribution(k)
#     plotEntityTS(k)

top_k_entities = 500
getTopKEntities(top_k_entities)

