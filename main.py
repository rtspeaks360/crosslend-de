# -*- coding: utf-8 -*-
# @Author: rish
# @Date:   2020-07-29 15:25:32
# @Last Modified by:   rish
# @Last Modified time: 2020-07-30 00:39:58


### Imports START
import os
import sys
import time


import parser
### Imports END


# Get script name and extract script path.
script_name = sys.argv[0]
script_path = script_name[:-8]

# Get arguments received
args = parser.parser_args()


if args.env == 'prod':
	print('prod environment')
	os.environ['ENV-INDICATOR'] = 'PROD'
	os.environ['SCPATH'] = script_path

	# Activate virtual environment with installed dependencies
	activate_this = script_path + 'env/bin/activate_this.py'
	with open(activate_this) as file_:
		exec(file_.read(), dict(__file__=activate_this))

	# Use project directory
	sys.path.insert(0, script_path)
else:
	os.environ['ENV-INDICATOR'] = 'DEV'


### Imports
from nyctlc_ingest import core as nyctlc


# [START Main function for the pipeline]
def main(args):
	'''
	Main function for the pipeline that handles the calls for other action
	specific functions.

	Args:
		-
	Returns:
		-
	'''

	if args.populate == 'pickup_zone':
		nyctlc.pickup_zones_by_passengers(
			args.export_path, args.month_identifier, args.top_k
		)

	elif args.populate == 'pickup_borough':
		nyctlc.pickup_borough_by_rides(
			args.export_path, args.month_identifier, args.top_k
		)

	elif args.initdb:
		nyctlc.utils.initialize_database()

	return
# [END]


if __name__ == '__main__':
	# Process start time
	process_start = time.time()

	print('Your namespace - ' + str(args))
	print()

	# Call for main function
	main(args)

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
