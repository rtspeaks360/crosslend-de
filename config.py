# -*- coding: utf-8 -*-
# @Author: rish
# @Date:   2020-07-29 15:25:38
# @Last Modified by:   rish
# @Last Modified time: 2020-07-29 16:34:01

### Imports START
import os
### Imports END

prod_string = 'postgres://hnepjqdz:CFbopyaJPIoFXEXbVPh8fmpw7xc5Gk18\
@balarama.db.elephantsql.com:5432/hnepjqdz'

if os.environ.keys().__contains__('ENV-INDICATOR') \
	and os.environ['ENV-INDICATOR'] == 'PROD':
	# Environment string
	env_str = 'PROD'
	DB_CONN_STRING = prod_string
else:
	# Environment string
	env_str = 'DEV'
	DB_CONN_STRING = 'postgresql+psycopg2://rish@localhost/crosslend'
