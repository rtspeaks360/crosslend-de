# -*- coding: utf-8 -*-
# @Author: rish
# @Date:   2020-07-29 15:25:38
# @Last Modified by:   rish
# @Last Modified time: 2020-07-30 00:19:50

### Imports START
import os
### Imports END

prod_string = 'postgres://hnepjqdz:CFbopyaJPIoFXEXbVPh8fmpw7xc5Gk18\
@balarama.db.elephantsql.com:5432/hnepjqdz'
pg_conn_string_remote = "dbname='hnepjqdz' user='hnepjqdz' \
host='balarama.db.elephantsql.com' password='CFbopyaJPIoFXEXbVPh8fmpw7xc5Gk18'\
port=5432"
pg_conn_string_local = "dbname='crosslend' user='rish' \
host='localhost' password='' port=5432"

if os.environ.keys().__contains__('ENV-INDICATOR') \
	and os.environ['ENV-INDICATOR'] == 'PROD':
	# Environment string
	env_str = 'PROD'
	DB_CONN_STRING = prod_string
	BASE_PATH = os.environ['SCPATH']
	PG_CONN_STRING = pg_conn_string_remote
else:
	# Environment string
	env_str = 'DEV'
	DB_CONN_STRING = 'postgresql+psycopg2://rish@localhost/crosslend'
	BASE_PATH = ''
	PG_CONN_STRING = pg_conn_string_local

LOCATION_MAP = BASE_PATH + 'data/taxi+_zone_lookup.csv'
