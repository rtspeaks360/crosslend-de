# -*- coding: utf-8 -*-
# @Author: rish
# @Date:   2020-07-29 15:26:49
# @Last Modified by:   rish
# @Last Modified time: 2020-07-29 17:54:10


### Imports START
import pandas as pd
### Imports END


# [START Load data for taxi rides]
def load_data(latest_export_path, month_identifier):
	pass
# [END]


# [START Function to get the final rides frame]
def get_master_rides_frame(latest_export_path, month_identifier):
	pass
# [END]


# [START Function to rank pickup zones by passenger counts]
def rank_zones_by_passengers(rides_frame, top_k, month_identifier):
	pass
# [END]


# [START Function to rank pickup borughs by ride counts]
def rank_boroughs_by_passengers(rides_frame, top_k, month_identifier):
	pass
# [END]


# [START]
def upsert_zone_ranks(zone_ranks, top_k):
	pass
# [END]


# [START Function to work out ranks to update and then insert]
def upsert_borough_ranks(borough_ranks, top_k):
	pass
# [END]
