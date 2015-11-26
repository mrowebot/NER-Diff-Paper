import json
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


def plot_cascade_patterns_distribution():
    cascade_type_frequency = {}

    # Read in the mention frequencies
    with open('../data/logs/cascade_shapes.tsv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            pattern = row[0].replace("[", "").replace("]", "").replace("'", "")
            # build graph from pattern
            local_di_graph = nx.DiGraph()
            for edge in pattern.split(","):
                source_node = edge.strip().split("->")[0]
                target_node = edge.strip().split("->")[1]
                local_di_graph.add_edge(source_node, target_node)

            # check if the local_graph has been added before - complexity: O(n^2)
            frequency = int(row[1])
            exists = False
            graph_key = local_di_graph
            for prior_key in cascade_type_frequency:
                if nx.is_isomorphic(graph_key, prior_key):
                    graph_key = prior_key
                    exists = True
                    break
            if not exists:
                cascade_type_frequency[graph_key] = frequency
            else:
                cascade_type_frequency[graph_key] += frequency

    data = {'counts': pd.Series(cascade_type_frequency.values(), index=cascade_type_frequency.keys())}
    df = pd.DataFrame(data)
    df = df.sort('counts', ascending=False)
    # for i in range(0, len(df)):
    #     print df.index[i].edges()
    #     print str(df.values[i][0])

    # Induce pattern rank distribution
    rank_data = {'counts': pd.Series(df.values.flatten(), index=range(1,len(df)+1))}
    rank_df = pd.DataFrame(rank_data)
    rank_df.plot(legend=False, logy=True, logx=True)
    plt.xlabel("log(Pattern Rank)")
    plt.ylabel("log(Frequency)")
    plt.savefig('../plots/cascade_rank_dist.pdf', bbox_inches='tight')
    plt.clf()

def plot_cascade_patterns_shapes():
    cascade_type_frequency = {}

    # Read in the mention frequencies
    with open('../data/logs/cascade_shapes.tsv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            pattern = row[0].replace("[", "").replace("]", "").replace("'", "")
            # build graph from pattern
            local_di_graph = nx.DiGraph()
            for edge in pattern.split(","):
                source_node = edge.strip().split("->")[0]
                target_node = edge.strip().split("->")[1]
                local_di_graph.add_edge(source_node, target_node)

            # check if the local_graph has been added before - complexity: O(n^2)
            frequency = int(row[1])
            exists = False
            graph_key = local_di_graph
            for prior_key in cascade_type_frequency:
                if nx.is_isomorphic(graph_key, prior_key):
                    graph_key = prior_key
                    exists = True
                    break
            if not exists:
                cascade_type_frequency[graph_key] = frequency
            else:
                cascade_type_frequency[graph_key] += frequency

    # plot the top 5 patterns
    k = 16
    data = {'counts': pd.Series(cascade_type_frequency.values(), index=cascade_type_frequency.keys())}
    df = pd.DataFrame(data)
    df = df.sort('counts', ascending=False)
    plt.figure(1)
    for i in range(1, k+1):
        plt.subplot(4, 4, i)
        # pos = nx.graphviz_layout(df.index[i], prog='dot')
        nx.draw(nx.reverse(df.index[i-1]), with_labels=False, node_size=50)
        plt.title("$r_{" + str(i) + "}$")
    plt.tight_layout()
    plt.savefig('../plots/cascade_shapes.pdf', bbox_inches='tight')
    plt.clf()


##### Main Execution code
# plot_cascade_patterns_distribution()
plot_cascade_patterns_shapes()
