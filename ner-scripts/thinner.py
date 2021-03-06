from __future__ import print_function

import codecs
import json
import locale
from pyspark import SparkContext, SparkConf
import re
import sys
import twokenize_wrapper

sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout) 
subreddits = ['4chan', 'worldnews', 'politics', 'soccer', 'technology', 'science', 'android', 'askscience', 'europe', 'sweden', 'gaming', 'movies', 'music', 'news', 'books', 'television', 'technology', 'sports', 'space', 'history', 'philosophy', 'art', 'programming', 'labouruk', 'harrypotter', 'conservative', 'unitedkingdom', 'formula1', 'lego', 'apple', 'seattle', 'libertarian', 'doctorwho', 'scifi', 'economics', 'travel', 'drugs', 'business', 'canada', 'environment', 'astronomy', 'batman', 'gardening', 'health', 'unitedkingdom', 'starwars', 'britishproblems', 'australia', 'engineering', 'casualconversation', 'startrek', 'literature', 'horror', 'google', 'chicago', 'linguistics', 'finance', 'coding', 'toronto', 'ireland', 'tea', 'thenetherlands', 'windows', 'energy', 'introvert', 'privacy', 'feminism', 'socialism', 'newzealand', 'france', 'politicalhumor', 'sanfrancisco', 'education', 'nhl', 'london', 'india', 'law', 'vegetarian', 'mexico', 'ukpolitics', 'worldevents', 'vancouver', 'bayarea', 'brasil', 'wikileaks', 'houston', 'boxing', 'atlanta', 'texas', 'liverpoolfc', 'anonymous', 'denver', 'rugbyunion', 'inthenews', 'germany', 'women', 'football', 'cricket', 'china', 'cycling', 'askacademia', 'chelseafc', 'israel']
subreddit_re = re.compile(r'subreddit":"(' + '|'.join(subreddits) + ')', re.I)

def thinposts(lines):

    posts = []

    for line in lines:
#        if not re.search(subreddit_re, line):
#            continue

        comment = json.loads(line)

        if comment['text'] == '[deleted]':
            continue
    
        if comment['community'][1]['name'].lower() in subreddits:
            out_comment = {}
            tokens = twokenize_wrapper.tokenize(comment['text'])
            out_comment['body'] = comment['text']
            out_comment['subreddit'] = comment['community'][1]['name']
            out_comment['author'] = comment['user']['username']
            out_comment['created_utc'] = str(int(comment['createdAt']) / 1000)
            out_comment['tokens'] = tokens
            out_comment['id'] = 'x'
            yield(json.dumps(out_comment))
#            yield reformed_text.strip() + ' <EOS>'

if __name__ == "__main__":

    inlocation = "hdfs://scc-culture-mind.lancs.ac.uk/reddit/Reddit-Compressed"
#    inlocation = "hdfs://scc-culture-mind.lancs.ac.uk/user/derczynskil/redditmini"
    outlocation = "hdfs://scc-culture-mind.lancs.ac.uk/user/derczynskil/data/reddit-cleaned-posts"

    ##### Main Execution Code
    conf = SparkConf().setAppName("Subreddit extraction")
    conf.set("spark.python.worker.memory","10g")
    conf.set("spark.driver.memory","15g")
    conf.set("spark.executor.memory","10g")
    conf.set("spark.default.parallelism", "12")
    conf.set("spark.mesos.coarse", "true")
    conf.set("spark.driver.maxResultSize", "10g")
    # Added the core limit to avoid resource allocation overruns
    conf.set("spark.cores.max", "10")
    conf.setMaster("mesos://zk://scc-culture-slave4.lancs.ac.uk:2181/mesos")
    conf.set("spark.executor.uri", "hdfs://scc-culture-mind.lancs.ac.uk/lib/spark-1.3.0-bin-hadoop2.4.tgz")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")

    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://scc-culture-mind.lancs.ac.uk/data/checkpointing")

    # get the HDFS url of the dataset
    dataset = "reddit"
    hdfsUrl = inlocation

    # broadcast the name of the dataset to the cluster
    print("----Broadcasting the name of the dataset being processed")
    datasetName = sc.broadcast(dataset)

    # run a map-reduce job to first compile the RDD for the dataset loaded from the file
    print("-----Dataset file: " + hdfsUrl)
    rawPostsFile = sc.textFile(hdfsUrl, minPartitions=12)

    # clean the posts and write them into HDFS from their respective paritions
    print("Writing to HDFS")
    outjson = rawPostsFile.mapPartitions(thinposts, preservesPartitioning=True).saveAsTextFile(outlocation)


    sc.stop()


