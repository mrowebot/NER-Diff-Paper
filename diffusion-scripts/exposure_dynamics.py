import json
from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf
import numpy as np
from scipy.sparse import csgraph

def computeExposureGraphForTop500Entities():

    ###### Spark Tranformation Functions
    # Returns: [(reply_id, orig_post_id)] - for flatMap - to avoid issue of 0 cardinality of the list
    def deriveReplyMap(json_line):
        reply_tuples = []
        json_obj = json.loads(json_line)
        if 'parent_id' in json_obj:
            orig_post_id = str(json_obj['parent_id'].encode("utf-8"))
            reply_post_id = str(json_obj['name'].encode("utf-8"))
            reply_tuples.append((reply_post_id, orig_post_id))
        return reply_tuples

    def combineReplies(replies1, replies2):
        replies = replies1 + replies2
        return replies

    def derivePostDetails(json_line):
        # get the entity posts
        # entity_posts_map = entity_posts_map_broadcast.value
        # entity_posts = []
        # for entity in entity_posts_map:
        #     entity_posts += entity_posts_map[entity]
        # entity_posts_set = set(entity_posts)
        # for entity in entity_posts_map:
        #     entity_posts_map[entity]
        # get the set of posts that cite the top-500 entities
        # entity_posts_set = [entity_posts_map[entity] for entity in entity_posts_map]

        # need to log: postid, userid, time of post
        json_obj = json.loads(json_line)
        post_id = str(json_obj['name'].encode("utf-8"))
        # if post_id in entity_posts_set:
        created_string = float(json_obj['created_utc'])
        created_date = datetime.fromtimestamp(created_string).date()
        user_id = str(json_obj['author'].encode("utf-8"))

        post_dict = {'user_id': user_id,
                     'post_id': post_id,
                     'created_date': created_date}
        return (post_id, post_dict)
        # else:
        #     return ("null", {})

    def deriveEntityToPosts(json_line):
        # Get the broadcast maps that need to be used
        orig_replies_map = orig_replies_map_broadcast.value
        reply_orig_map = reply_orig_map_broadcast.value
        top_500_entities = set(top_500_entities_broadcast.value)

        entity_posts = []
        json_obj = json.loads(json_line)
        post_id = json_obj['name']
        entities = json_obj['entity_texts']
        for entity in entities:
            # ensure that the entities is one that we want to process
            if entity in top_500_entities:
                try:
                    # ensure that the post appears in a chain - in order to filter out singleton citations
                    if post_id in orig_replies_map or post_id in reply_orig_map:
                        entity_posts.append((str(entity), [str(post_id)]))
                except:
                    pass
        return entity_posts


    # input: tuple x where x[0] = entity name, x[1] = entity posts
    def derive_per_entity_exposure_distribution(x):
        entity = x[0]
        posts = x[1]

        # get the posts for this entity from the broadcast variable
        # entity_posts_list = entity_posts_map_broadcast.value[entity]
        entity_posts_list = posts
        post_details_map = post_details_broadcast.value

        # time order the posts of the entity
        print("Generating time-ordered posts and users citing the entity")
        date_to_posts = {}
        post_users = []
        for post in entity_posts_list:
            # check that the post has been by an existing user
            post_user = post_details_map[post]['user_id']
            if '[deleted]' not in post_user and post in post_details_map:
                post_users.append(post_user)
                print(post_user)
                post_date = post_details_map[post]['created_date']
                # get the timestamp
                if post_date in date_to_posts:
                    date_to_posts[post_date] += [post]
                else:
                    date_to_posts[post_date] = [post]

        # get the user specific replies
        entity_users = set(post_users)
        print("Collecting the user specific time series interaction graph")
        reply_orig_map = reply_orig_map_broadcast.value
        # Get the: {user, {date, interacted_user}} map
        user_to_interaction_dates = {}
        for reply_post_id in reply_orig_map:
            if post_details_map[reply_post_id]['user_id'] in entity_users:
                # Check that we have the post details from the reply map
                if reply_orig_map[reply_post_id] in post_details_map and reply_post_id in post_details_map:
                    user_id = post_details_map[reply_post_id]['user_id']
                    interaction_date = post_details_map[reply_post_id]['created_date']
                    interacted_user = str(post_details_map[reply_orig_map[reply_post_id]]['user_id'])
                    if '[deleted]' not in interacted_user:
                        if user_id in user_to_interaction_dates:
                            interaction_dates = user_to_interaction_dates[user_id]
                            if interaction_date in interaction_dates:
                                interaction_dates[interaction_date] += [interacted_user]
                            else:
                                interaction_dates[interaction_date] = [interacted_user]
                            user_to_interaction_dates[user_id] = interaction_dates
                        else:
                            user_to_interaction_dates[user_id] = {interaction_date: [interacted_user]}



        # Go through and derive the point of activation of each user
        activation_points = {}
        activated_users = set()
        for user in entity_users:
            # get times when they interacted with people
            # dict: {date, [user_id]}
            if user in user_to_interaction_dates:
                ts_interactions = user_to_interaction_dates[user]

                # go through the posts of the entity each day
                for date in sorted(date_to_posts):
                    # only do the computation if the user is not activated already
                    if user not in activated_users:
                        # Go through each post on the sorted dates
                        date_posts = date_to_posts[date]
                        for post in date_posts:
                            # is the post by the user - if so, this will be the user's first citation of the entity: activated
                            if post_details_map[post]['user_id'] is user:
                                # get how many times the user was exposed to the entity before adopting it
                                prior_post_authors = []
                                for prior_date in sorted(date_to_posts):
                                    if prior_date < date:
                                        # get all the users who authored posts before this one
                                        for prior_post in date_to_posts[prior_date]:
                                            prior_post_authors.append(post_details_map[prior_post]['user_id'])

                                # get all of the posts that were authored by people that the person had replied to in the past
                                prior_users = set()
                                for ts_date in sorted(ts_interactions):
                                    if ts_date < date:
                                        print("TS Date = " + str(ts_date))
                                        print(str(ts_interactions))
                                        for prior_user in ts_interactions[ts_date]:
                                            prior_users.add(prior_user)
                                exposure_count = 0
                                # count how many times the user was exposed to the entity (given that they had interacted with the users beforehand
                                for prior_author in prior_post_authors:
                                    if prior_author in prior_users:
                                        exposure_count += 1

                                # log the exposure count
                                if exposure_count in activation_points:
                                    activation_points[exposure_count] += 1
                                else:
                                    activation_points[exposure_count] = 1

                                # Exit the posts dates loop
                                activated_users.add(user)
                                break

        # Return the mapping between the entity and the activation point distribution
        return (entity, activation_points)


    ###### Execution code
    conf = SparkConf().setAppName("NER Diffusion - Exposure Dynamics")
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
    # annotationFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/annotated-sample")
    annotationFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/annotated")
    annotationFile.cache()
    thinnedFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json")
    # thinnedFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/thinned-json-sample")
    thinnedFile.cache()

    # Top 500 entities file
    top500EntitiesFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/reddit/entities/top500_entities.csv")
    top500Entities = top500EntitiesFile.map(lambda x: str(x.encode("utf-8"))).collect()
    print("Top Entities Loaded. Total = " + str(len(top500Entities)))
    top_500_entities_broadcast = sc.broadcast(top500Entities)
    # print(str(len(top500Entities)))
    # print(top500Entities)

    # Load the reply graphs from the thinnedFile
    reply_map_rdd = thinnedFile\
        .flatMap(deriveReplyMap)
    reply_map_rdd.cache()

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

    # Load the entity to post map - restricted to the top-500 entities
    # input: json_line of annotations of each post
    # output: [(entity, [post])] of posts where entities appeared
    entity_posts_rdd = annotationFile\
        .flatMap(deriveEntityToPosts)\
        .reduceByKey(lambda p1, p2: p1 + p2)
    entity_posts_rdd_map = entity_posts_rdd.collectAsMap()
    # entity_posts_map_broadcast = sc.broadcast(entity_posts_rdd_map)
    print("Entity to posts Map Size = " + str(len(entity_posts_rdd_map)))
    # print(entity_posts_rdd_map)

    # Get the post details - restricted to the top-500 entities for now
    post_details = thinnedFile\
        .map(derivePostDetails)\
        .filter(lambda x: x[0] is not "null")\
        .collectAsMap()
    post_details_broadcast = sc.broadcast(post_details)
    print("Post Details Size = " + str(len(post_details)))
    # print(post_details)

    # Compute the exposure curves for each entity in the dataset
    entity_exposure_curves_rdd = entity_posts_rdd\
        .map(derive_per_entity_exposure_distribution)
    entity_exposure_curves_rdd_distribution = entity_exposure_curves_rdd\
        .collect()
    # print(entity_exposure_curves_rdd_distribution)

    print("Entity Exposure Curves Distribution Size = " + str(len(entity_exposure_curves_rdd_distribution)))
    output_string = ""
    for (entity, dist_dictionary) in entity_exposure_curves_rdd_distribution:
        output_string += str(entity)
        for exposure_count in dist_dictionary:
            output_string += "\t" + str(exposure_count) + ", " + str(dist_dictionary[exposure_count])
        output_string += "\n"
    print(output_string)
    outputFile = open("data/exposure_curves.tsv", "w")
    outputFile.write(output_string)
    outputFile.close()

    # stop the Spark context from running
    sc.stop()




#### Main code execution area
if __name__ == "__main__":
    computeExposureGraphForTop500Entities()
