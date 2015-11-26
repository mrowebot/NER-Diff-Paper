import json
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf
import numpy as np
from scipy.sparse import csgraph

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
        # Get the broadcast maps that need to be used
        # orig_replies_map = orig_replies_map_broadcast.value
        reply_orig_map = reply_orig_map_broadcast.value
        orig_replies_map = orig_replies_map_broadcast.value

        entity_posts = []
        json_obj = json.loads(json_line)
        post_id = json_obj['name']
        entities = json_obj['entity_texts']

        # get the replies to this post
        for entity in entities:
            try:
                # ensure that the post appears in a chain - in order to filter out singleton citations
                if post_id in orig_replies_map or post_id in reply_orig_map:
                # if len(orig_replies_map.lookup(post_id)) > 0 \
                #         or len(reply_orig_map.lookup(post_id)) > 0:    # New code to use rdd lookup to save RDD partitioning
                    entity_posts.append((str(entity), [str(post_id)]))
            except:
                pass
        return entity_posts


    ###### Graph Isomorphism Functions

    # Input: (entity,[chains]) - tuple
    # output: (entity, [connected_chain]) - tuple
    def induce_type1_cascades(tuple):
        entity = tuple[0]
        chains = tuple[1]
        # entity_posts = tuple[1]['posts']

        # Get the entity posts
        entity_citation_posts = entity_posts_map_broadcast.value
        entity_posts = entity_citation_posts[entity]

        # log the connected chains of the entity
        connected_chains = []
        for chain in chains:
            # new chain formed by filtering the chain with only entity citing posts
            # Filter the chain down to include only posts citing the entity
            new_chain = set()
            for chain_edge in chain:
                source = chain_edge.split("->")[0]
                target = chain_edge.split("->")[1]
                if source in entity_posts and target in entity_posts:
                    new_chain.add(chain_edge)

            # too inefficient, better to convert to matrix form and then run this the graph
            # 1. Induce maps between node label and ids
            node_to_index = {}
            index_to_node = {}
            index = -1
            for chain_edge in new_chain:
                source = chain_edge.split("->")[0]
                target = chain_edge.split("->")[1]

                if source not in node_to_index:
                    index += 1
                    node_to_index[source] = index
                    index_to_node[index] = source

                if target not in node_to_index:
                    index += 1
                    node_to_index[target] = index
                    index_to_node[index] = target


            # 2. Populate the nd matrix
            dim = len(node_to_index)
            if dim > 1:
                M = np.zeros(shape=(dim, dim))
                for chain_edge in new_chain:
                    source = chain_edge.split("->")[0]
                    source_index = node_to_index[source]
                    target = chain_edge.split("->")[1]
                    target_index = node_to_index[target]
                    M[source_index, target_index] = 1

                # 3. Induce the connected components from the matrix
                # print(str(dim))
                # print(str(M))
                Msp = csgraph.csgraph_from_dense(M, null_value=0)
                n_components, labels = csgraph.connected_components(Msp, directed=True)
                # print("Number of connected components = " + str(n_components))
                # print("Components labels = " + str(labels))
                # get the components and their chains
                for i in range(0, n_components):
                    # print("Component: " + str(i))
                    component_chain = []
                    # get the nodes in that component
                    # print(labels)
                    c_nodes = [j for j in range(len(labels)) if labels.item(j) is i]
                    # print(c_nodes)
                    # Only log the component if more than two nodes are in in
                    if len(c_nodes) > 1:
                        # build the canonical edges
                        for source_id in c_nodes:
                            for target_id in c_nodes:
                                if int(M[(source_id, target_id)]) is 1:
                                    component_chain.append(str(source_id) + "->" + str(target_id))
                        if len(component_chain) > 0:
                            connected_chains.append(component_chain)

            canonical_chains = connected_chains
            print("Canonical Chains:")
            print(canonical_chains)


        # return back to the function the mapping between the entity and the connected chains
        return (entity, canonical_chains)

    # Input: (entity, [post])
    # Output: [(entity, {cascades: [cascade]})]
    def computePerEntityCascadesAndPosts(tuple):
        entity = tuple[0]
        posts = tuple[1]

        # Get the broadcast maps that need to be used
        orig_replies_map = orig_replies_map_broadcast.value
        reply_orig_map = reply_orig_map_broadcast.value

        # Get the cascade graphs for each entity
        entity_chains = []
        chain_posts = set()

        # get the chain that each post was in
        for post in posts:
            # Do this to speed up computation by ensuring that we haven't already recorded the post in another chain
            if post not in chain_posts:
                chain_posts.add(post)
                chain = []
                to_process = []

                # Starting from the seed post in a possible chain
                # get the replies to the post - down the chain
                if post in orig_replies_map:
                    replies = orig_replies_map[post]
                    to_process += replies
                    for reply in replies:
                        chain.append(reply + "->" + post)
                # Get the post that the post replied to
                if post in reply_orig_map:
                    orig_post = reply_orig_map[post]
                    to_process += orig_post
                    chain.append(post + "->" + orig_post)

                # Go through each post that is to be processed
                while len(to_process) > 0:
                    to_process_post = to_process.pop()
                    # log that the post has been processed in an already found chain
                    chain_posts.add(to_process_post)
                    # get the replies to this post
                    if to_process_post in orig_replies_map:
                        replies = orig_replies_map[to_process_post]
                        to_process += replies
                        for reply in replies:
                            chain.append(reply + "->" + to_process_post)
                    # get the post that this post relied to
                    if to_process_post in reply_orig_map:
                        orig_post = reply_orig_map[to_process_post]
                        to_process += orig_post
                        chain.append(to_process_post + "->" + orig_post)

                # log the chain for the entity
                entity_chains.append(chain)

        # Return the entity chains
        return (entity, entity_chains)

    ###### Execution code
    conf = SparkConf().setAppName("NER Diffusion - Cascade Pattern Mining")
    conf.set("spark.python.worker.memory","10g")
    conf.set("spark.driver.memory","15g")
    conf.set("spark.executor.memory","10g")
    conf.set("spark.default.parallelism", "12")
    conf.set("spark.mesos.coarse", "true")
    conf.set("spark.driver.maxResultSize", "10g")
    conf.set("spark.cores.max", "15")
    conf.setMaster("mesos://zk://scc-culture-slave9.lancs.ac.uk:2181/mesos")
    conf.set("spark.executor.uri", "hdfs://scc-culture-mind.lancs.ac.uk/lib/spark-1.3.0-bin-hadoop2.4.tgz")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")

    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://scc-culture-mind.lancs.ac.uk/data/checkpointing")

    # use sample directory for testing
    annotationFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/annotated-sample")
    # annotationFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/annotated")
    annotationFile.cache()
    # thinnedFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json")
    thinnedFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json-sample")
    thinnedFile.cache()

    # Load the reply graphs from the thinnedFile
    print("Loading replies map")
    reply_map_rdd = thinnedFile\
        .flatMap(deriveReplyMap)

    # Collect as a map and broadcast this to the cluster
    reply_orig_map = reply_map_rdd\
        .collectAsMap()
    print("Reply Orig Map Size = " + str(len(reply_orig_map)))
    # print(reply_orig_map)
    reply_orig_map_broadcast = sc.broadcast(reply_orig_map)

    # # get the: {orig, [reply]} dictionary
    orig_replies_map = reply_map_rdd\
        .map(lambda x: (x[1], [x[0]]))\
        .reduceByKey(combineReplies)\
        .collectAsMap()
    print("Orig Replies Map Size = " + str(len(orig_replies_map)))
    orig_replies_map_broadcast = sc.broadcast(orig_replies_map)


    # Load the entity to post map
    # input: json_line of annotations of each post
    # output: [(entity, [post])] of posts where entities appeared
    print("Loading entity to posts rdd")
    entity_posts_rdd = annotationFile\
        .flatMap(deriveEntityToPosts)\
        .reduceByKey(lambda p1, p2: p1 + p2)
    entity_posts_rdd_map = entity_posts_rdd.collectAsMap()
    print("Entity to posts map size = " + str(len(entity_posts_rdd_map)))
    entity_posts_map_broadcast = sc.broadcast(entity_posts_rdd_map)
    # print("Entity to posts Map Size = " + str(entity_posts_rdd.count()))
    # entity_posts_rdd_sample = sc.parallelize(entity_posts_rdd.take(100))
    # # print(entity_posts_rdd_map)
    #
    print("Computing entity cascades")
    # Output: (entity, {"cascades": entity_chains, "posts": posts})
    entity_cascades_rdd = entity_posts_rdd\
        .map(computePerEntityCascadesAndPosts)
    # #     # .reduceByKey(lambda chain1, chain2: chain1 + chain2)
    entity_cascades_rdd_map = entity_cascades_rdd.collect()
    print("Entity cascades Map Size = " + str(len(entity_cascades_rdd_map)))
    #
    print("Inducing distribution of cascade shapes - Global")
    # Induce global distributiion
    # Returns: [(entity, [chain])]
    canonical_cascade_patterns = entity_cascades_rdd\
        .map(induce_type1_cascades)
    # # canonical_cascade_patterns_distribution_map = canonical_cascade_patterns\
    # #     .take(10)
    # # print("Entity connected cacades Map Size = " + str(len(canonical_cascade_patterns_distribution_map)))
    # # print(canonical_cascade_patterns_distribution_map)

    print("Top Patterns:")
    canonical_cascade_patterns_distribution = canonical_cascade_patterns\
        .flatMap(lambda x: x[1] if len(x) is 2 else ["null"])\
        .map(lambda x: (str(x), 1))\
        .filter(lambda x: "null" not in x[0])\
        .reduceByKey(lambda count1, count2: count1 + count2)\
        .map(lambda x: (x[1], x[0]))\
        .sortByKey(False)\
        .map(lambda x: (x[1], x[0]))
    top_patterns = canonical_cascade_patterns_distribution.collect()
    # print(top_patterns)
    #
    # Write the patterns to local disk for local isomorphism computation
    outputString = ""
    for (pattern, freq) in top_patterns:
        outputString += str(pattern) + "\t" + str(freq) + "\n"
    outputFile = open("data/cascade_shapes.tsv", "w")
    outputFile.write(outputString)
    outputFile.close()

    # stop the Spark context from running
    sc.stop()



#### Main code execution area
if __name__ == "__main__":
    computeGlobalCascadeIsomorphicDistribution()