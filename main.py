# -*- coding: utf-8 -*-
# @Author: rish
# @Date:   2020-07-29 15:25:32
# @Last Modified by:   rish
# @Last Modified time: 2020-07-29 15:53:23


### Imports START
import argparse
import sys
import time
### Imports END


# Get script name and extract script path.
script_name = sys.argv[0]
script_path = script_name[:-8]


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
		'--env', choices=['dev', 'prod'], default='dev',
		help='Use this argument to specify whether the processes are to be run in a\
		development environment or production.'
	)

	return parser
# [END]


# [START Main function for the pipeline]
def main():
	'''
	Main function for the pipeline that handles the calls for other action
	specific functions.

	Args:
		-
	Returns:
		-
	'''

	args = parser.argument_parser_main()

	return
# [END]


if __name__ == '__main__':
	# Process start time
	process_start = time.time()

	# Call for main function
	main()

	process_time = time.time() - process_start
	mins = int(process_time / 60)
	secs = int(process_time % 60)

	print(
		'Total time consumed: {mins} minutes {secs} seconds'
		.format(mins=mins, secs=secs)
	)
	print('')
	print('-*-*-*-*-*-*-*-*-*-*-*-*-END-*-*-*-*-*-*-*-*-*-*-*-*-')
	print('')