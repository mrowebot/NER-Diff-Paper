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

def plotTauDistribution():
    # taus = []
    tau_dist = {}
    total_freq = 0

    # Read in the mention frequencies
    print "Computing discrete frequency distribution for the taus"
    with open('../data/logs/taus_dist.tsv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            try:
                tau = int(float(row[0]) / 1000 / 60 / 60)
                if tau in tau_dist:
                    tau_dist[tau] += 1
                else:
                    tau_dist[tau] = 1
                total_freq += 1
            except:
                pass

    print "Computing the data frame"
    rel_freqs_dist = {key: (float(value)/float(total_freq)) for (key, value) in tau_dist.items()}
    data = {'counts': pd.Series(rel_freqs_dist.values(),
                                index=rel_freqs_dist.keys())}
    # Compute the average tau
    avg_tau = 0
    for tau in rel_freqs_dist:
        avg_tau += tau * rel_freqs_dist[tau]
    # print str(avg_tau)

    # data = {'counts': pd.Series(tau_dist.values(),
    #                              index=tau_dist.keys())}
    df = pd.DataFrame(data)
    df = df.sort('counts', ascending=False)

    print "Plotting"
    # plot the full distibution with full logs on x and y
    fig = plt.figure()
    ax = plt.gca()
    ax.plot(df.index, df['counts'], '.')
    plt.axvline(x=avg_tau, color="darkgreen", linestyle="--", linewidth=float(2))
    plt.text(avg_tau+1000, .001, r'$\mu=$' + str(int(avg_tau)), color="darkgreen")
    ax.set_yscale('log')
    ax.set_xscale('log')
    plt.xlabel(r"log($\tau_{v,u}$) - hours")
    # plt.xlabel("$\tau_{v,u}$")
    plt.ylabel(r"log($p_{\tau_{v,u}}$)")
    plt.savefig('../plots/taus_dist_full_log.pdf', bbox_inches='tight')
    plt.clf()

    fig = plt.figure()
    ax = plt.gca()
    ax.plot(df.index, df['counts'], '.')
    plt.axvline(x=avg_tau, color="darkgreen", linestyle="--", linewidth=float(2))
    plt.text(avg_tau+1000, .001, r'$\mu=$' + str(int(avg_tau)), color="darkgreen")
    ax.set_yscale('log')
    # ax.set_xscale('log')
    plt.xlabel(r"$\tau_{v,u}$ - hours")
    plt.ylabel(r"log($p_{\tau_{v,u}}$)")
    plt.savefig('../plots/taus_dist_y_log.pdf', bbox_inches='tight')
    plt.clf()

    # plot a zoomed in version of the distribution
    fig = plt.figure()
    ax = plt.gca()
    ax.plot(df.index, df['counts'], '.')
    plt.axvline(x=avg_tau, color="darkgreen", linestyle="--", linewidth=float(2))
    plt.text(avg_tau+1000, .001, r'$\mu=$' + str(int(avg_tau)), color="darkgreen")
    plt.xlabel(r"$\tau_{v,u}$ - hours")
    plt.ylabel(r"$p_{\tau_{v,u}}$")
    plt.savefig('../plots/taus_dist_no_logs.pdf', bbox_inches='tight')
    plt.clf()




### Execution code
plotTauDistribution()