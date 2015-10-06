from __future__ import print_function

from pyspark import SparkContext, SparkConf
from LineParser import LineParser
from Post import Post

if __name__ == "__main__":

    ##### Utility functions
    def getHDFSFileLocation(dataset_name):
        if dataset_name is "facebook":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/facebook/facebook-posts.tsv"
        elif dataset_name is "boards":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/boards/boards-posts.tsv"
        elif dataset_name is "reddit":
            return "hdfs://scc-culture-mind.lancs.ac.uk/reddit/uncompressed"
        elif dataset_name is "twitter":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/kershad1/data/twitter/tweets2.json"

    def getHDFSCleanedFileLocation(dataset_name):
        if dataset_name is "facebook":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/facebook/facebook-cleaned-posts"
        elif dataset_name is "boards":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/boards/boards-cleaned-posts"
        elif dataset_name is "reddit":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/reddit/reddit-cleaned-posts"
        elif dataset_name is "twitter":
            return "hdfs://scc-culture-mind.lancs.ac.uk/user/kershad1/data/twitter/tweets-cleaned-posts"

    def cleanLines(lines):
        posts_global = []

        # receive the broadcast variables
        dataset_name = datasetName.value
        tokensDictBroadcastV = tokensDictBroadcast.value
        stopwords = stopwordsSet.value

        for line in lines:
            if "facebook" in dataset_name:
                posts = LineParser.parseFacebookLine(line)
                posts_global += posts
            elif "boards" in dataset_name:
                posts = LineParser.parseBoardsLine(line)
                posts_global += posts
                # count += len(posts)
            elif "reddit" in dataset_name:
                posts = LineParser.parseRedditLine(line, dataset_name)
                posts_global += posts
            elif "twitter" in dataset_name:
                posts = LineParser.parseTwitterLine(line, dataset_name)
                posts_global += posts

        # Clean each line that has been loaded
        minFreq = 5
        newPosts = []
        for post in posts_global:
            newMessage = ""
            postContent = post.content.lower()

            # prep the content by removing punctuation
            postContent = postContent.replace("'", "").replace(".", "").replace(",", "")
            # remove the square brackets content
            postContent = postContent.replace("[b]", "").replace("[/b]", "")
            postContent = postContent.replace("[i]", "").replace("[/i]", "")
            postContent = postContent.replace("[quote]", "").replace("[/quote]", "")
            postContent = postContent.replace("[url]", "").replace("[/url]", "")

            terms = postContent.split()
            for term in terms:
                if tokensDictBroadcastV[term] >= minFreq and term not in stopwords:
                    # Create the new post
                    newMessage += term + " "
                    
            # Check that the new message is not just blank
            if len(newMessage) > 0:
                newPost = Post(post.author, post.postid, post.forumid, post.date)
                newPost.addContent(newMessage)
                newPosts.append(newPost.toTSVString())

        # Write the new posts to HDFS

        return [newPosts]
        # return [count]

    def combineListsLengths(count1, count2):
        return count1 + count2

    ##### Data Cleaning Spark Functions
    def lineTokenizer(line):
        dataset_name = datasetName.value
        posts = []

        if "facebook" in dataset_name:
            posts = LineParser.parseFacebookLine(line)
        elif "boards" in dataset_name:
            posts = LineParser.parseBoardsLine(line)
        elif "reddit" in dataset_name:
            posts = LineParser.parseRedditLine(line, dataset_name)
        elif "twitter" in dataset_name:
            posts = LineParser.parseTwitterLine(line, dataset_name)
        terms = []
        if len(posts) == 1:
            # tokenizer = RegexpTokenizer(r'\w+')
            # terms = posts[0].content.lower().split()
            postContent = posts[0].content.lower()

            # prep the content by removing punctuation
            postContent = postContent.replace("'", "").replace(".", "").replace(",", "")
            # remove the square brackets content
            postContent = postContent.replace("[b]", "").replace("[/b]", "")
            postContent = postContent.replace("[i]", "").replace("[/i]", "")
            postContent = postContent.replace("[quote]", "").replace("[/quote]", "")
            postContent = postContent.replace("[url]", "").replace("[/url]", "")

            terms = postContent.split()
        return terms

    # Compiles a dictionary of terms using a basic term count distribution and MR design pattern
    def tokenFrequencyMapper(token):
        return (token, 1)

    def tokenFrequencyReducer(count1, count2):
        count = count1 + count2
        return count

    ##### Main Execution Code
    conf = SparkConf().setAppName("StochFuse - Dataset Cleaning")
    conf.set("spark.python.worker.memory","10g")
    conf.set("spark.driver.memory","15g")
    conf.set("spark.executor.memory","10g")
    conf.set("spark.default.parallelism", "12")
    conf.set("spark.mesos.coarse", "true")
    conf.set("spark.driver.maxResultSize", "10g")
    # Added the core limit to avoid resource allocation overruns
    conf.set("spark.cores.max", "10")
#    conf.setMaster("mesos://zk://scc-culture-mind.lancs.ac.uk:2181/mesos")
    conf.setMaster("mesos://zk://scc-culture-slave4.lancs.ac.uk:2181/mesos")
    conf.set("spark.executor.uri", "hdfs://scc-culture-mind.lancs.ac.uk/lib/spark-1.3.0-bin-hadoop2.4.tgz")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")

    sc = SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://scc-culture-mind.lancs.ac.uk/data/checkpointing")

    # set the datasets to be processed
    # datasets = ["facebook"]
    datasets = ["reddit"]


    # clean each dataset
    for dataset in datasets:
        # get the HDFS url of the dataset
        hdfsUrl = getHDFSFileLocation(dataset)

        # broadcast the name of the dataset to the cluster
        print("----Broadcasting the name of the dataset being processed")
        datasetName = sc.broadcast(dataset)

        # Load the stopwords file from hdfs
        print("----Loading stopwords file and broadcasting to the cluster")
        stopwordsFile = sc.textFile("hdfs://scc-culture-mind.lancs.ac.uk/user/rowem/data/stopwords.csv")
        stopwords = stopwordsFile.map(lambda x: [str(x)]).reduce(lambda x, y: x + y)
        print("---Stopwords: %s" % str(stopwords))
        print("---Stopwords Length: %s" % str(len(stopwords)))
        stopwordsSet = sc.broadcast(stopwords)

        # run a map-reduce job to first compile the RDD for the dataset loaded from the file
        print("-----Dataset file: " + hdfsUrl)
        rawPostsFile = sc.textFile(hdfsUrl, minPartitions=12)

        # Go through each partition and then count how many posts are stored within each
        print("-----Computing partition-level MR job..")

        # Effort 2: running mapPartitions
        # y = rawPostsFile.mapPartitions(lineMapperLists).collect()

        # Working examples:
        # y = rawPostsFile.mapPartitions(lineMapperLists).reduce(combineListsLengths)
        # y = rawPostsFile.mapPartitions(cleanLines, preservesPartitioning=True).collect()

        # get the token dictionary
        tokensDict = rawPostsFile\
            .flatMap(lineTokenizer)\
            .map(tokenFrequencyMapper)\
            .reduceByKey(tokenFrequencyReducer)\
            .sortByKey()\
            .collectAsMap()
        print("Tokens dictionary size: %s" % str(len(tokensDict)))
        # print("Tokens dictionary: %s" % str(tokensDict))

        # broadcast the token dictionary to the cluster
        tokensDictBroadcast = sc.broadcast(tokensDict)

        # test pulling a record from the RDD
        # print("Testing key entry to pull value: %s" % str(tokensDict['pro-ogitive']))

        # clean the posts and write them into HDFS from their respective paritions
        print("Writing to HDFS")
        cleanFileLocation = getHDFSCleanedFileLocation(dataset)
        cleanedLines = rawPostsFile.mapPartitions(cleanLines, preservesPartitioning=True)\
            .saveAsTextFile(cleanFileLocation)
        # print("Cleaned output from partitions: %s" % str(len(cleanedLines)))
            # .collect()

        # Save the file to HDFS
        # cleanFileLocation = getHDFSCleanedFileLocation(dataset)
        # print("Writing to HDFS")
        # cleanedLines_RDD = sc.parallelize(cleanedLines).collect()


        sc.stop()


