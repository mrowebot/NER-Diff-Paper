#!/usr/bin/env python
# takes one arg: path to a conll-format file w/ words and one tag column indicating label (e..g crfsuite output)
import sys

conll = open(sys.argv[1], 'r')

debug = False

outside_tag = 'O'

last_tag = None
entities = set([])
current_entity = []


for line in conll:
	line = line.strip()
	if not line:
		tag = None
	else:
		token, tag = line.split()
		if debug:
			print line

	if tag != last_tag:
		if tag == 'O' and current_entity:
			# store entity
			entities.add(' '.join(current_entity))
			if debug:
				print "--------------- found", ' '.join(current_entity)
			current_entity = []
		elif tag != 'O':
			current_entity.append(token)

if current_entity:
	entities.add(' '.join(current_entity))

print entities	
