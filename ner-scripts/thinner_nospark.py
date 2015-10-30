#!/usr/bin/env python
import codecs
import json
import locale
import re
import sys

sys.stdout = codecs.getwriter(locale.getpreferredencoding())(sys.stdout) 
subreddits = ['4chan', 'worldnews', 'politics', 'soccer', 'technology', 'science', 'android', 'askscience', 'europe', 'sweden', 'gaming', 'movies', 'music', 'news', 'books', 'television', 'technology', 'sports', 'space', 'history', 'philosophy', 'art', 'programming', 'labouruk', 'harrypotter', 'conservative', 'unitedkingdom', 'formula1', 'lego', 'apple', 'seattle', 'libertarian', 'doctorwho', 'scifi', 'economics', 'travel', 'drugs', 'business', 'canada', 'environment', 'astronomy', 'batman', 'gardening', 'health', 'unitedkingdom', 'starwars', 'britishproblems', 'australia', 'engineering', 'casualconversation', 'startrek', 'literature', 'horror', 'google', 'chicago', 'linguistics', 'finance', 'coding', 'toronto', 'ireland', 'tea', 'thenetherlands', 'windows', 'energy', 'introvert', 'privacy', 'feminism', 'socialism', 'newzealand', 'france', 'politicalhumor', 'sanfrancisco', 'education', 'nhl', 'london', 'india', 'law', 'vegetarian', 'mexico', 'ukpolitics', 'worldevents', 'vancouver', 'bayarea', 'brasil', 'wikileaks', 'houston', 'boxing', 'atlanta', 'texas', 'liverpoolfc', 'anonymous', 'denver', 'rugbyunion', 'inthenews', 'germany', 'women', 'football', 'cricket', 'china', 'cycling', 'askacademia', 'chelseafc', 'israel']
subreddit_re = re.compile(r'subreddit":"(' + '|'.join(subreddits) + ')', re.I)

for line in open(sys.argv[1]):
    if not re.search(subreddit_re, line):
        continue

    comment = json.loads(line)

    if comment['body'] == '[deleted]':
        continue

    if comment['subreddit'].lower() in subreddits:
        print(json.dumps(comment))

