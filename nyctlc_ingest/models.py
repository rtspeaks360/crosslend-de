# -*- coding: utf-8 -*-
# @Author: rish
# @Date:   2020-07-29 15:26:45
# @Last Modified by:   rish
# @Last Modified time: 2020-07-29 22:09:18


### Imports START
from sqlalchemy import create_engine, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, BigInteger, Integer, DateTime
from sqlalchemy.sql import func
### Imports END


# Object for the delcaratove base class
Base = declarative_base()


### Model Class to define the data base structure START


# [START ZoneHistory class to define the zone_history table]
class ZoneHistory(Base):
	'''
	Model class that defines the structure for the zone_history table for
	the monthly ranks for zones based on passengers are stored.
	'''

	__tablename__ = 'zone_history'

	_id = Column(BigInteger, primary_key=True)

	month = Column(String(7), nullable=False)
	pick_up = Column(String(300), nullable=False)
	drop_off = Column(String(300), nullable=False)
	rank = Column(Integer, nullable=False)

	insert_time = Column(DateTime, nullable=False, default=func.now())
	update_time = Column(
		DateTime, nullable=False,
		default=func.now(),
		onupdate=func.current_timestamp()
	)

	UniqueConstraint(
		month, pick_up, drop_off, rank,
		name='unique_zone_rank_per_month'
	)
# [END]


# [START BoroughHistory class to define the borough_history table]
class BoroughHistory(Base):
	'''
	Model class that defines the structure for the borough_history table for
	the monthly ranks for zones based on passengers are stored.
	'''

	__tablename__ = 'borough_history'

	_id = Column(BigInteger, primary_key=True)

	month = Column(String(7), nullable=False)
	pick_up = Column(String(300), nullable=False)
	drop_off = Column(String(300), nullable=False)
	rank = Column(Integer, nullable=False)

	insert_time = Column(DateTime, nullable=False, default=func.now())
	update_time = Column(
		DateTime, nullable=False,
		default=func.now(),
		onupdate=func.current_timestamp()
	)

	UniqueConstraint(
		month, pick_up, drop_off, rank,
		name='unique_borough_rank_per_month'
	)
# [END]


# [START Code to create the database from all the above schema classes]
def convert_classes_into_tables(connection_string):
	'''
	Function that maps and creates the tables in the databse from all
	the above schema.

	Args:
		- connection string
	Returns:
		-
	'''

	engine = create_engine(connection_string)
	Base.metadata.create_all(engine)
	print('created_tables')
	return
# [END]


### Queries for views Q-4
latest_zone_ranks = '''
CREATE VIEW latest_zone_ranks as (
	WITH t1 as(
		SELECT
			*,
			ROW_NUMBER() OVER (
				PARTITION BY pick_up, rank ORDER BY month DESC
			) AS rn,
			ROW_NUMBER() OVER (
				PARTITION BY pick_up, drop_off ORDER BY month DESC
			) AS rn2
		FROM zone_history
	)
	SELECT
		pick_up, drop_off, rank
	FROM t1 where rn = 1 and rn2 = 1
)
'''

latest_borough_ranks = '''
CREATE VIEW latest_borough_ranks as (
	WITH t1 as(
		SELECT
			*,
			ROW_NUMBER() OVER (
				PARTITION BY pick_up, rank ORDER BY month DESC
			) AS rn,
			ROW_NUMBER() OVER (
				PARTITION BY pick_up, drop_off ORDER BY month DESC
			) AS rn2
		FROM borough_history
	)
	SELECT
		pick_up, drop_off, rank
	FROM t1 where rn = 1 and rn2 = 1
)
'''

### Creating schema in database START
if __name__ == '__main__':
	import context
	convert_classes_into_tables(context.config.DB_CONN_STRING)
### Creating schema in database END
