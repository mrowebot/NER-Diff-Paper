import json
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf


def computeGlobalCascadeIsomorphicDistribution():
    ###### Spark Tranformation Functions
    # Returns: [(reply_id, orig_post_id)] - for flatMap - to avoid issue of 0 cardinality of the list
    def deriveReplyMap(json_line):
        reply_tuples = []
        json_obj = json.loads(json_line)
        if 'parent_id' in json_obj:
            orig_post_id = json_obj['parent_id']
            reply_post_id = json_obj['name']
            reply_tuples.append((reply_post_id, orig_post_id))
        return reply_tuples

    def combineReplies(replies1, replies2):
        replies = replies1 + replies2
        return replies


    ###### Execution code
    conf = SparkConf().setAppName("NER Diffusion - Cascade Pattern Mining")
    conf.set("spark.python.worker.memory","10g")
    conf.set("spark.driver.memory","15g")
    conf.set("spark.executor.memory","10g")
    conf.set("spark.default.parallelism", "12")
    conf.set("spark.mesos.coarse", "true")
    conf.set("spark.driver.maxResultSize", "10g")
    conf.set("spark.cores.max", "5")
    conf.setMaster("mesos://zk://scc-culture-slave9.lancs.ac.uk:2181/mesos")
    conf.set("spark.executor.uri", "hdfs://scc-culture-mind.lancs.ac.uk/lib/spark-1.3.0-bin-hadoop2.4.tgz")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")

    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://scc-culture-mind.lancs.ac.uk/data/checkpointing")

    # use sample directory for testing
    annotationFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/annotated")
    annotationFile.cache()
    thinnedFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json")
    thinnedFile.cache()

    # Load the reply graphs from the thinnedFile
    reply_map_rdd = thinnedFile\
        .flatMap(deriveReplyMap)\
        .reduceByKey()
    reply_map_rdd.cache()

    # get the: {reply, orig} dictionary
    reply_orig_map = reply_map_rdd\
        .collectAsMap()
    print("Reply Orig Map Size = " + str(len(reply_orig_map)))
    # get the: {orig, [reply]} dictionary
    orig_replies_map = reply_map_rdd\
        .map(lambda x: (x[1], x[0]))\
        .reduceByKey(combineReplies)\
        .collectAsMap()
    print("Orig Replies Map Size = " + str(len(orig_replies_map)))
    #
    #
    #
    # entities = []
    # posts = []
    # cascades = {}
    # for entity in entities:
    #     # get the posts that the entity was cited in:
    #     for post in posts:
    #         # was the post in a chain?
    #         # if so, get the parent of the chain
    #         # induce the graph of the chain in which entities were repeatedly cited - using breadth-first search
    #         # induce the entity cascade throughout the graph - induce isomorphic form and record




#### Main code execution area
if __name__ == "__main__":
    computeGlobalCascadeIsomorphicDistribution()