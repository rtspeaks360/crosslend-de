# -*- coding: utf-8 -*-
# @Author: rish
# @Date:   2020-07-29 16:55:41
# @Last Modified by:   rish
# @Last Modified time: 2020-07-29 17:15:34

### Imports
import argparse


# [START Function to define parser]
def parser_args():
	'''
	Function to define the structure for command line arguments parser.

	Args:
		-
	Retuns:
		-
	'''

	parser = argparse.ArgumentParser(
		description='Pipeline to ingest the monthly new york city taxi rides\
		data exports, perfom required aggregations to get the monthly rankings\
		and then update them in the database.'
	)

	parser.add_argument(
		'--populate', choices=['pickup_zone', 'pickup_borough'],
		dest='populate', help='Choose of the options to either rank pickup zones\
		by passenger counts (2-i), or pickup pickup_borough by ride counts'
	)

	parser.add_argument(
		'--month_identifier', dest='month_identifier',
		help='Use this argument to specify the month for the export.'
	)

	parser.add_argument(
		'--export_path', dest='export_path',
		help='Use this argument to specify the export path.'
	)

	parser.add_argument(
		'--top_k', dest='top_k', help='Use this argument to specify the number of\
		top entries you want to limit.'
	)

	parser.add_argument(
		'--env', choices=['dev', 'prod'], default='dev',
		help='Use this argument to specify whether the processes are to be run in a\
		development environment or production.'
	)

	# Parsing the arguments received
	args = parser.parse_args()

	return args
# [END]
