__author__ = 'rowem'

import json
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf
import re
import pandas as pd
import matplotlib.pyplot as plt

def computeEntityTSData():

    ##### Spark Functions
    # input: json_line
    # [(entity: {date, count})]
    def deriveEntityToDate(json_line):
        entity_dates = []

        json_obj = json.loads(json_line)
        created_string = float(json_obj['created_utc'])
        created_date = datetime.fromtimestamp(created_string).date()
        annotations = json_obj['entity_texts']
        for annotation in annotations:
            date_count = {}
            date_count[created_date] = 1
            entity_dates.append((annotation, date_count))
        return entity_dates

    def combineDateCounts(date_counts1, date_counts2):
        date_counts = date_counts1
        for date in date_counts2:
            if date in date_counts:
                date_counts[date] += date_counts2[date]
            else:
                date_counts[date] = date_counts2[date]
        return date_counts


    ###### Execution code
    conf = SparkConf().setAppName("NER Diffusion - Exploratory Plots")
    conf.set("spark.python.worker.memory","10g")
    conf.set("spark.driver.memory","15g")
    conf.set("spark.executor.memory","10g")
    conf.set("spark.default.parallelism", "12")
    conf.set("spark.mesos.coarse", "true")
    conf.set("spark.driver.maxResultSize", "10g")
    # Added the core limit to avoid resource allocation overruns
    conf.set("spark.cores.max", "5")
    conf.setMaster("mesos://zk://scc-culture-slave9.lancs.ac.uk:2181/mesos")
    conf.set("spark.executor.uri", "hdfs://scc-culture-mind.lancs.ac.uk/lib/spark-1.3.0-bin-hadoop2.4.tgz")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")

    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://scc-culture-mind.lancs.ac.uk/data/checkpointing")

    # use sample directory for testing
    # distFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/user/derczynskil/RC_2015-01")
    distFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/annotated")
    # Point to local file until data has finished uploading to HDFS
    # distFile = sc.textFile("/home/derczynskil/annotated/")
    distFile.cache()

    # Step 1: Derive the time-sensitive map of when entities appeared
    print("----Loading entity time-series")
    entity_citation_dates = distFile\
        .flatMap(deriveEntityToDate)\
        .reduceByKey(combineDateCounts)
    entity_citation_dates.cache()
    # print(entity_citation_dates.collect())

    print("----Deriving the count of entity citations")
    entity_citation_counts = entity_citation_dates\
        .map(lambda x: (x[0], len(x[1])))\
        .map(lambda x: (x[1], x[0]))\
        .sortByKey(False)\
        .map(lambda x: (x[1], x[0]))\
        .collect()

    # Write to local disk
    print("------Writing the output to a file")
    outputString = ""
    for (entity, count) in entity_citation_counts:
        outputString += str(entity.encode('utf-8')).replace("'", "") + "\t" + str(count) + "\n"
    # print(outputString)
    outputFile = open("data/entity_mention_frequencies.csv", "w")
    outputFile.write(outputString)
    outputFile.close()

    # Write the time-series output to local disk
    print("------Writing the ts output to a file")
    outputString = ""
    for (entity, date_to_count) in entity_citation_dates.collect():
        outputString += str(entity.encode('utf-8')).replace("'", "")
        for date in date_to_count:
            outputString += "\t" + str(date) + "|" + str(date_to_count[date])
        outputString += "\n"
    # print(outputString)
    outputFile = open("data/entity_mention_ts.csv", "w")
    outputFile.write(outputString)
    outputFile.close()

    # stop the Spark context from running
    sc.stop()

#### Main code execution area
if __name__ == "__main__":
    computeEntityTSData()
