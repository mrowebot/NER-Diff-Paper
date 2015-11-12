import csv
from pandas import Series
import numpy as np
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import sys
import networkx.algorithms.isomorphism as iso
import networkx as nx
from matplotlib.patches import FancyArrowPatch

def plot_exposure_curves():
    exposure_count_dist = {}
    total_freq = 0

    # Read in the mention frequencies
    with open('../data/logs/exposure_curves.tsv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            entity = row[0]
            for i in range(1, len(row)):
                exposure_count = row[i].split(",")[0]
                freq = int(row[i].split(",")[1])
                total_freq += freq

                # map the exposure count to how many times this has occurred
                if exposure_count in exposure_count_dist:
                    exposure_count_dist[exposure_count] += freq
                else:
                    exposure_count_dist[exposure_count] = freq

    # plot the distribution
    exposure_rel_freqs_dist = {key: (float(value)/float(total_freq)) for (key, value) in exposure_count_dist.items()}
    data = {'counts': pd.Series(exposure_rel_freqs_dist.values(),
                                index=exposure_rel_freqs_dist.keys())}
    df = pd.DataFrame(data)

    # plot the full distibution with full logs on x and y
    df = df.sort('counts', ascending=False)
    df.plot(legend=False, logy=True, logx=True)
    plt.xlabel("log($k$)")
    plt.ylabel("log($p_k$)")
    plt.savefig('../plots/exposure_count_dist_full_log.pdf', bbox_inches='tight')
    plt.clf()

    # plot the main distribution - with y-log
    df = df.sort('counts', ascending=False)
    df.plot(legend=False, logy=True)
    plt.xlabel("$k$")
    plt.ylabel("log($p_k$)")
    plt.savefig('../plots/exposure_count_dist_y_log.pdf', bbox_inches='tight')
    plt.clf()

    # plot a zoomed in version of the distribution
    df.plot(legend=False)
    plt.xlabel("$k$")
    plt.ylabel("$p_k$")
    plt.savefig('../plots/exposure_count_dist_no_logs.pdf', bbox_inches='tight')
    plt.clf()

def plot_top_k_exposure_curves(k):
    entity_exposure_count_dist = {}
    entity_adoption_counts = {}

    # Read in the mention frequencies
    with open('../data/logs/exposure_curves.tsv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            total_freq = 0
            entity = row[0]
            exposure_count_dist = {}
            for i in range(1, len(row)):
                exposure_count = row[i].split(",")[0]
                freq = int(row[i].split(",")[1])
                total_freq += freq

                # map the exposure count to how many times this has occurred
                if exposure_count in exposure_count_dist:
                    exposure_count_dist[exposure_count] += freq
                else:
                    exposure_count_dist[exposure_count] = freq
            entity_exposure_count_dist[entity] = exposure_count_dist
            entity_adoption_counts[entity] = total_freq

    # determine the top k entities
    adoption_data = {'counts': pd.Series(entity_adoption_counts.values(), index=entity_adoption_counts.keys())}
    adoption_df = pd.DataFrame(adoption_data)
    adoption_df = adoption_df.sort('counts', ascending=False)
    top_entities = [adoption_df.index[i] for i in range(0, k)]

    # plot each entities exposure curve
    matplotlib.rcParams.update({'font.size': 8})
    plt.figure(1)
    plot_count = 1
    for entity in top_entities:
        print entity
                # plot the distribution
        exposure_count_dist = entity_exposure_count_dist[entity]
        total_freq = sum(exposure_count_dist.values())
        exposure_rel_freqs_dist = {key: (float(value)/float(total_freq)) for (key, value) in exposure_count_dist.items()}
        data = Series(exposure_rel_freqs_dist.values(), index=exposure_rel_freqs_dist.keys()).sort_index(ascending=True)
        # data = Series.sort_index(ascending=True)
        # df = pd.DataFrame(data)
        # df = df.sort(ascending=False)

        # plot the main distribution - with y-log
        print str(plot_count)
        plt.subplot(3, 3, plot_count)
        data.plot(legend=False)
        plt.xlabel("$k$")
        plt.ylabel("$p_k$")
        plt.title(entity)

        plot_count += 1


    plt.tight_layout()
    plt.savefig('../plots/per_entity_exposure_count_dist_y_log.pdf', bbox_inches='tight')
    plt.clf()

#### Main Execution code
# plot_exposure_curves()
k = 9
plot_top_k_exposure_curves(9)
