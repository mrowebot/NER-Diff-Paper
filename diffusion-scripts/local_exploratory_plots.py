__author__ = 'rowem'
import json
from post import Post
from datetime import datetime
import operator
import pandas as pd
import matplotlib.pyplot as plt
from pandas import Series
import numpy as np

# import the reddit posts from the denoted local file
def importPosts(file):
    reddit_posts = []

    jsonFileRead = open(file, 'r')
    for line in jsonFileRead:
        # print line
        json_obj = json.loads(line)

        # initialisation variables
        author = json_obj['author']
        forumid = json_obj['subreddit_id']
        postid = json_obj['id']
        created_string = float(json_obj['created_utc'])
        created_date = datetime.fromtimestamp(created_string)
        post = Post(author, postid, forumid, created_date)

        # secondary variables
        text = json_obj['body']
        post.addContent(text)
        annotations = json_obj['entity_texts']
        post.addAnnotations(annotations)

        reddit_posts.append(post)
    return reddit_posts

def plotPerEntityDistribution(posts, k):
    entity_mentions = {}
    for post in posts:
        for annotation in post.annotations:
            if annotation in entity_mentions:
                entity_mentions[annotation] += 1
            else:
                entity_mentions[annotation] = 1

    data = {'counts': pd.Series(entity_mentions.values(), index=entity_mentions.keys())}
    df = pd.DataFrame(data)
    df = df.sort('counts', ascending=False)
    df[0:(k-1)].plot(kind='bar', legend=False)
    plt.savefig('../data/plots/entity_freq_top_k_' + str(k) + '.pdf', bbox_inches='tight')


def plotEntityTSDistribution(posts, k):
    entity_mentions = {}
    for post in posts:
        for annotation in post.annotations:
            if annotation in entity_mentions:
                entity_mentions[annotation] += 1
            else:
                entity_mentions[annotation] = 1

    data = {'counts': pd.Series(entity_mentions.values(), index=entity_mentions.keys())}
    df = pd.DataFrame(data)
    df = df.sort('counts', ascending=False)

    # # plot the frequency distribution
    # plotting_dir = "../data/plots/"
    # sorted_entity_mentions = sorted(entity_mentions.items(), key=operator.itemgetter(1), reverse=True)
    # entity_mentions_top_k = {}
    # count = 1
    # for entity in sorted_entity_mentions:
    #     # print entity[0]
    #     # print entity[1]
    #     entity_mentions_top_k[entity[0]] = entity[1]
    #     count += 1
    #     if count > k:
    #         break
    # # sort the top k by value
    # sorted_entity_mentions_top_k = sorted(entity_mentions_top_k.items(), key=operator.itemgetter(1), reverse=True)
    # #
    # x_values = []
    # y_values = []
    # for entity in sorted_entity_mentions_top_k:
    #     print entity[0]
    #     x_values.append(entity[0])
    #     y_values.append(entity[1])
    #
    # plt.bar(range(len(sorted_entity_mentions_top_k)), y_values, align="center")
    # plt.xticks(range(len(sorted_entity_mentions_top_k)), x_values)
    # # plt.xlim([0, k])
    # locs, labels = plt.xticks()
    # plt.setp(labels, rotation=90)
    # plt.savefig('../data/plots/entity_freq_top_k_' + str(k) + '.pdf', bbox_inches='tight')




### Main execution code
# load the reddit posts
local_test_file_path = "../data/annotated/RC_2013-04.100k.annotated.json"
posts = importPosts(local_test_file_path)
print len(posts)

# run basic exploratory analyses
ks = [20, 40, 60, 80, 100]
for k in ks:
    plotPerEntityDistribution(posts, k)

top_k_diffusion = 5

