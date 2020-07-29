# -*- coding: utf-8 -*-
# @Author: rish
# @Date:   2020-07-29 15:26:49
# @Last Modified by:   rish
# @Last Modified time: 2020-07-29 23:02:27


### Imports START
import pandas as pd
import psycopg2 as pg
from sqlalchemy.dialects import postgresql
from sqlalchemy import exc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config
from nyctlc_ingest import models
### Imports END


### Global declarations
COLUMNS_TO_LOAD = [
	'PULocationID', 'DOLocationID', 'passenger_count',
	'trip_distance', 'total_amount'
]
FILENAMES = {
	'green_trip': {
		'columns_map': {
			'lpep_pickup_datetime': 'pick_up_datetime',
			'lpep_dropoff_datetime': 'drop_off_datetime'
		},
		'identifier': '/green_tripdata_{mi}.csv'
	},
	'yellow_trip': {
		'columns_map': {
			'tpep_pickup_datetime': 'pick_up_datetime',
			'tpep_dropoff_datetime': 'drop_off_datetime'

		},
		'identifier': '/yellow_tripdata_{mi}.csv'
	}
}

COLUMNS = [
	'pick_up_datetime', 'drop_off_datetime', 'PULocationID',
	'DOLocationID', 'passenger_count', 'trip_distance', 'total_amount'
]

engine = create_engine(config.DB_CONN_STRING)
models.Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)


# [START Function to get the location map]
def _get_location_map():
	'''
	Function to return the location map frame from the csv.

	Args:
		-
	Returns:
		- location_map frame
	'''
	location_map = pd.read_csv(config.LOCATION_MAP)
	location_map = location_map.loc[location_map.Borough != 'Unknown']
	return location_map
# [END]


# [START Load data for taxi rides]
def _load_data(latest_export_path, month_identifier, fileindetifier):
	'''
	Load the taxi data csvs into memory as pd frames based on the file
	identifier, do the required preprocessing and return the required
	data.

	Args:
		- latest_export path
		- month_identifier
		- fileidentifier
	Returns:
		- processed trip data frame
	'''

	df = pd.read_csv(
		latest_export_path + FILENAMES[fileindetifier]['identifier'].format(
			mi=month_identifier
		),
		usecols=(COLUMNS_TO_LOAD + list(
			FILENAMES[fileindetifier]['columns_map'].keys()
		))
	)
	df.rename(columns=FILENAMES[fileindetifier]['columns_map'], inplace=True)
	df = df[COLUMNS]
	return df
# [END]


# [START Function to get the final rides frame]
def get_master_rides_frame(latest_export_path, month_identifier):
	'''
	Function to get the trip data from the Yellow trip and Green trip CSVs,
	do the required preprocessing and then create a final master rides frame
	with locations strings.

	Args:
		- latest_export_path
		- month_identifier
	'''

	# Creating master frame
	frames = []
	for _ in FILENAMES.keys():
		frames.append(
			_load_data(latest_export_path, month_identifier, _)
		)

	master_rides_fm = pd.concat(frames)
	master_rides_fm.reset_index(drop=True, inplace=True)

	# Get location map
	location_map = _get_location_map()

	# Merge with location map to get locations texts for pickup locations
	master_rides_fm = pd.merge(
		master_rides_fm, location_map.rename(
			columns={
				'Borough': 'pick_up_borough',
				'Zone': 'pick_up_zone',
				'service_zone': 'pick_up_service_zone'
			}
		), left_on='PULocationID', right_on='LocationID',
		how='left'
	)
	master_rides_fm.drop('LocationID', axis=1, inplace=True)

	# Merge with location map to get locations texts for dropoff locations
	master_rides_fm = pd.merge(
		master_rides_fm,
		location_map.rename(
			columns={
				'Borough': 'drop_off_borough',
				'Zone': 'drop_off_zone',
				'service_zone': 'drop_off_service_zone'
			}
		), left_on='DOLocationID', right_on='LocationID',
		how='left'
	)
	master_rides_fm.drop('LocationID', axis=1, inplace=True)

	print('Size of master_frame - ' + str(master_rides_fm.shape[0]))

	return master_rides_fm
# [END]


# [START Function to rank pickup zones by passenger counts]
def rank_zones_by_passengers(rides_frame, top_k, month_identifier):
	'''
	Function to rank the pick_up_zone - drop_off_zone tuples by their
	popularity based on number of passengers, select the top k drop_off_zone
	for each pick_up_zone and return the ranking required.

	Args:
		- rides_frame
		- top_k
		- month_identifier
	Return:
		- ranking
	'''
	# Passenger coutns for each tuple
	ranking = rides_frame.groupby(by=['pick_up_zone', 'drop_off_zone']).sum()
	ranking.reset_index(inplace=True)
	ranking.sort_values(
		by=['pick_up_zone', 'passenger_count'],
		ascending=[True, False], inplace=True
	)

	# Selecting targert columns and generating ranking
	ranking = ranking[['pick_up_zone', 'drop_off_zone', 'passenger_count']]
	ranking.reset_index(drop=True, inplace=True)
	ranking['rank'] = (
		ranking
		.groupby(['pick_up_zone'])['passenger_count']
		.rank(method='first', ascending=False)
		.astype(int)
	)

	# Selecting top_k ranks for each pick_up_zone
	ranking = ranking.groupby('pick_up_zone').head(top_k)

	ranking['month'] = month_identifier
	ranking.drop('passenger_count', axis=1, inplace=True)

	ranking.rename(
		columns={'pick_up_zone': 'pick_up', 'drop_off_zone': 'drop_off'},
		inplace=True
	)

	print('Number of ranks generatred - ' + str(ranking.shape[0]))
	return ranking
# [END]


# [START Function to rank pickup borughs by ride counts]
def rank_boroughs_by_rides(rides_frame, top_k, month_identifier):
	'''
	Function to rank the pick_up_borough - drop_off_borough tuples by their
	popularity based on number of rides, select the top k drop_off_borough
	for each pick_up_borough and return the ranking required.

	Args:
		- rides_frame
		- top_k
		- month_identifier
	Return:
		- ranking
	'''

	# Ride counts for each tuple
	ranking = (
		rides_frame
		.groupby(by=['pick_up_borough', 'drop_off_borough'])
		.count()
	)
	ranking.reset_index(inplace=True)
	ranking = ranking[['pick_up_borough', 'drop_off_borough', 'pick_up_datetime']]
	ranking.rename(columns={'pick_up_datetime': 'rides'}, inplace=True)
	ranking.sort_values(
		['pick_up_borough', 'rides'],
		ascending=[True, False], inplace=True
	)
	ranking.reset_index(drop=True, inplace=True)

	# Generating ranks based on rides for each pick_up_borough
	ranking['rank'] = (
		ranking
		.groupby('pick_up_borough')['rides']
		.rank(method='first', ascending=False)
		.astype(int)
	)

	ranking['month'] = month_identifier
	ranking.drop('rides', axis=1, inplace=True)

	ranking.rename(
		columns={'pick_up_borough': 'pick_up', 'drop_off_borough': 'drop_off'},
		inplace=True
	)

	print('Number of ranks generatred - ' + str(ranking.shape[0]))
	return ranking
# [END]


# [START]
def upsert_zone_ranks(zone_ranks):
	'''
	Function to work out the ranks to be updated into the database
	after comparing the current ranks to already recorded ranks and
	then  do the inserts for the changes.

	Args:
		- ranking for zones
	Returns:
		-
	'''

	# Get the current ranks
	ranking_db = pd.read_sql('SELECT * FROM latest_zone_ranks', con=engine)

	# Ranks changed from previously recorded info
	upsert_master = pd.merge(
		zone_ranks, ranking_db, on=['pick_up', 'drop_off', 'rank'],
		how='left', indicator='Exist'
	)
	upsert_master = upsert_master.loc[upsert_master.Exist == 'left_only']
	upsert_master.drop('Exist', axis=1, inplace=True)

	# If more than one ranks updated
	if upsert_master.shape[0] > 0:
		# Initialize session and update ranking in db
		session = DBSession()

		# Insert statement
		insert_stmt = (
			postgresql
			.insert(models.ZoneHistory.__table__)
			.values(upsert_master.to_dict(orient='records'))
		)

		try:
			session.execute(insert_stmt)
			session.commit()
			print(
				'Number of rank changes pushed into the db - ' + str(
					upsert_master.shape[0]
				)
			)
		except exc.SQLAlchemyError as e:
			print(e._message)
			session.rollback()

		session.close()
	else:
		print('No new rank changes found.')

	return
# [END]


# [START Function to work out ranks to update and then insert]
def upsert_borough_ranks(borough_ranks):
	'''
	Function to work out the ranks to be updated into the database
	after comparing the current ranks to already recorded ranks and
	then  do the inserts for the changes.

	Args:
		- ranking for boroughs
	Returns:
		-
	'''
	# Get the current ranks
	ranking_db = pd.read_sql('SELECT * FROM latest_borough_ranks', con=engine)

	# Ranks changed from previously recorded info
	upsert_master = pd.merge(
		borough_ranks, ranking_db, on=['pick_up', 'drop_off', 'rank'],
		how='left', indicator='Exist'
	)
	upsert_master = upsert_master.loc[upsert_master.Exist == 'left_only']
	upsert_master.drop('Exist', axis=1, inplace=True)

	# If more than one ranks updated
	if upsert_master.shape[0] > 0:
		# Initialize session and update ranking in db
		session = DBSession()

		# Insert statement
		insert_stmt = (
			postgresql
			.insert(models.BoroughHistory.__table__)
			.values(upsert_master.to_dict(orient='records'))
		)

		try:
			session.execute(insert_stmt)
			session.commit()
			print(
				'Number of rank changes pushed into the db - ' + str(
					upsert_master.shape[0]
				)
			)
		except exc.SQLAlchemyError as e:
			print(e._message)
			session.rollback()

		session.close()
	else:
		print('No new rank changes found.')

	return
# [END]
