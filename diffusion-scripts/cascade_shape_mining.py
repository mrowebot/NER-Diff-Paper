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

    def deriveEntityToPosts(json_line):
        entity_posts = []
        json_obj = json.loads(json_line)
        post_id = json_obj['name']
        entities = json_obj['entity_texts']
        for entity in entities:
            try:
                entity_posts.append((str(entity), str(post_id)))
            except:
                pass
        return entity_posts


    ###### Graph Isomorphism Functions
    # input: list of posts that the entity has appeared in + list of chains in which the entity has been cited
    # output: list of canonical entity chains
    def induce_type1_cascades(entity_citation_posts, chains):
        for chain in chains:
            # new chain formed by filtering the chain with only entity citing posts
            new_chain = set()
            for chain_edge in chain:
                source = chain_edge.split("->")[0]
                target = chain_edge.split("->")[1]
                if source in entity_citation_posts and target in entity_citation_posts:
                    new_chain.add(chain_edge)
            # Ensure that the chain is connected after filtering
            filtered_chain = set()
            filterd_source_set = [chain_edge.split("->")[0] for chain_edge in new_chain]
            filterd_target_set = [chain_edge.split("->")[1] for chain_edge in new_chain]
            for chain_edge in new_chain:
                source = chain_edge.split("->")[0]
                target = chain_edge.split("->")[1]





    ###### Execution code
    conf = SparkConf().setAppName("NER Diffusion - Cascade Pattern Mining")
    conf.set("spark.python.worker.memory","10g")
    conf.set("spark.driver.memory","15g")
    conf.set("spark.executor.memory","10g")
    conf.set("spark.default.parallelism", "12")
    conf.set("spark.mesos.coarse", "true")
    conf.set("spark.driver.maxResultSize", "10g")
    conf.set("spark.cores.max", "10")
    conf.setMaster("mesos://zk://scc-culture-slave9.lancs.ac.uk:2181/mesos")
    conf.set("spark.executor.uri", "hdfs://scc-culture-mind.lancs.ac.uk/lib/spark-1.3.0-bin-hadoop2.4.tgz")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")

    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://scc-culture-mind.lancs.ac.uk/data/checkpointing")

    # use sample directory for testing
    annotationFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/annotated")
    annotationFile.cache()
    # thinnedFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json")
    thinnedFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json-sample")
    thinnedFile.cache()

    # Load the reply graphs from the thinnedFile
    reply_map_rdd = thinnedFile\
        .flatMap(deriveReplyMap)
    reply_map_rdd.cache()

    # get the: {reply, orig} dictionary
    reply_orig_map = reply_map_rdd\
        .collectAsMap()
    print("Reply Orig Map Size = " + str(len(reply_orig_map)))
    # # get the: {orig, [reply]} dictionary
    orig_replies_map = reply_map_rdd\
        .map(lambda x: (x[1], x[0]))\
        .reduceByKey(combineReplies)\
        .collectAsMap()
    print("Orig Replies Map Size = " + str(len(orig_replies_map)))

    # Load the entity to post map
    # input: json_line of annotations of each post
    # output: [(entity, [post])] of posts where entities appeared
    entity_posts_rdd = annotationFile\
        .flatMap(deriveEntityToPosts)\
        .reduceByKey(lambda p1, p2: p1 + p2)
    entity_posts_rdd_map = entity_posts_rdd.collect()
    print("Entity to posts Map Size = " + str(len(entity_posts_rdd_map)))

    #
    #
    # orig_replies_map = {}
    # # entities = []
    # # posts = []
    #
    # # Get the cascade graphs for each entity
    # for entity in entity_posts_rdd_map:
    #     entity_chains = []
    #     chain_posts = set()
    #
    #     # get the chain that each post was in
    #     for post in entity_posts_rdd_map[entity]:
    #         # Do this to speed up computation by ensuring that we haven't already recorded the post in another chain
    #         if post not in chain_posts:
    #             chain_posts.add(post)
    #             chain = set()
    #             toProcess = []
    #
    #             # Starting from the seed post in a possible chain
    #             # get the replies to the post - down the chain
    #             if post in orig_replies_map:
    #                 replies = orig_replies_map[post]
    #                 toProcess += replies
    #                 chain.add([reply + "->" + post for reply in replies])
    #             # Get the post that the post replied to
    #             if post in reply_orig_map:
    #                 orig_post = reply_orig_map[post]
    #                 toProcess += orig_post
    #                 chain.add(post + "->" + orig_post)
    #
    #             # Go through each post that is to be processed
    #             while len(toProcess) > 0:
    #                 to_process_post = toProcess.pop()
    #                 # log that the post has been processed in an already found chain
    #                 chain_posts.add(to_process_post)
    #                 # get the replies to this post
    #                 if to_process_post in orig_replies_map:
    #                     replies = orig_replies_map[to_process_post]
    #                     toProcess += replies
    #                     chain.add([reply + "->" + to_process_post for reply in replies])
    #                 # get the post that this post relied to
    #                 if to_process_post in reply_orig_map:
    #                     orig_post = reply_orig_map[to_process_post]
    #                     toProcess += orig_post
    #                     chain.add(to_process_post + "->" + orig_post)
    #             # log the chain for the entity
    #             if entity in entity_chains:
    #                 entity_chains[entity] += sorted(chain)
    #             else:
    #                 entity_chains[entity] = sorted(chain)

    # Record the entity patterns depending on the type of pattern to infer and log the canonical form
    # Type 1: strict consecutive patterns
    # Type 2: 2-hop scan bounds (i.e. transitive link)
    # Type 3: keep bounds, highlight citations









    #     # get the posts that the entity was cited in:
    #     for post in posts:
    #         # was the post in a chain?
    #         # if so, get the parent of the chain
    #         # induce the graph of the chain in which entities were repeatedly cited - using breadth-first search
    #         # induce the entity cascade throughout the graph - induce isomorphic form and record




#### Main code execution area
if __name__ == "__main__":
    computeGlobalCascadeIsomorphicDistribution()