# -*- coding: utf-8 -*-
# @Author: rish
# @Date:   2020-07-29 15:26:36
# @Last Modified by:   rish
# @Last Modified time: 2020-07-29 20:07:06

### Imports START
from nyctlc_ingest import utils
### Imports END


# [START Procedure for 2-i]
def pickup_zones_by_passengers(
	export_path, month_identifier, top_k
):
	'''
	Procedure pickup_zones_by_passengers to get the final rides dataframe,
	rank the pick_up - drop_off zone tuples by passenger counts, work out the
	ranks that need to be pushed into the database and then do the insert

	Args:
		- export_path
		- month_identifier
		- top_k
	Returns:
		-
	'''
	master_rides_fm = utils.get_master_rides_frame(export_path, month_identifier)
	print('Loaded trip data into master frame\n')

	ranks_by_passenger_counts = utils.rank_zones_by_passengers(
		master_rides_fm, top_k, month_identifier
	)
	print('Computed latest ranking based on passenger counts\n')

	utils.upsert_zone_ranks(ranks_by_passenger_counts, top_k)
	print('Pushed the new computed ranks into the database\n')
	return
# [END]


# [START Procedure for 2-ii]
def pickup_borough_by_rides(
	export_path, month_identifier, top_k
):
	'''
	Procedure pickup_borough_by_rides to get the final rides dataframe,
	rank the pick_up - drop_off borough tuples by ride counts, work out the ranks
	that need to be pushed into the database and then do the insert.

	Args:
		- export_path
		- month_identifier
		- top_k
	Returns:
		-
	'''
	master_rides_fm = utils.get_master_rides_frame(export_path, month_identifier)
	print('Loaded trip data into master frame\n')

	rank_by_ride_counts = utils.rank_boroughs_by_passengers(
		master_rides_fm, top_k, month_identifier
	)
	print('Computed latest ranking based on passenger counts\n')

	utils.upsert_borough_ranks(rank_by_ride_counts, top_k)
	print('Pushed the new computed ranks into the database\n')
	return
# [END]
