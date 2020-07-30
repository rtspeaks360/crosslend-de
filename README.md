# Crosslend Data Engineering challenge
This is the solution repository for the crosslend data engineering task. For the purpose of this task, I have designed an ETL job that:
* Loads the taxi trips data from New York City Taxi & Limousine Commission (NYC TLC) public dataset for a given month
* Generates two types of rankings (2-1) and (2-2) from the data engineering challenge
* Does the upsert, based on the new ranking records that need to be inserted in the database.

Technologies Used: Python(Pandas, SqlAlchemy, Psycopg2, Argsparse), Postrges(ElephantSQL in Production - Postgrest as a service)

## Instructions for using the solution application
To set up the application on your machine
* Clone the repository on your local and create a python3 virtualenv environment, and then install the dependencies.
  * `virtualenv env python3`
  * `source env/bin/activate`
  * `pip install -r requirements`
* Update your database credentials in the config file.
* Use the python application in `initialize_db` mode to initialize the database structure including models and views.
  command - `python main.py --env prod --initialize_db`
* Initialize a `data` directory, where you can put your monthly export data. Also download the location_map (look up table) csv from the website and update the path for the csv in the config file.
* Your database has been initialized, now you can use the application to do the monthly imports using the populate command.
* And example command to rank the monthly trip data for 2019-10 looks like:  
```python main.py --populate pickup_zone --month_identifier 2019-10 --top_k 50 --export_path data/2019-10 --env prod```
  * In the above command  we use `populate` arg to specify whether we want to rank zones by passengers or boroughs by ride count.
  * `month_identifier` is used to identify the two trip data files (yellow_trip data and green trip data) in the data export directory.
  * `top_k` is used to specify how many top records for each zone do we want to keep.
  * `export_path` path of the export directory where the latest trip data is stored.
  * `env` evironment argument to specify if you wanna do the updates in the local/development environment or in the production environment.

For more usage information have a look at the application parser description.

<img src="https://github.com/rtspeaks360/crosslend-de/blob/solution-v1/ss/NYCTLC_INGEST_INFO.png?raw=true" width="500" height="400" />

Also feel free to have a look in the ss directory for output from various runs [here](https://github.com/rtspeaks360/crosslend-de/tree/solution-v1/ss).

<img src="https://github.com/rtspeaks360/crosslend-de/blob/solution-v1/ss/003.png?raw=true" width="1300" height="400" />

## To answer the task specific questions
* The data for task 2-i and 2-ii is computed by the ETL process and is updated into the database tables, `zone_history` and `borough_history` in a way that satisfies the requirement from task 3.
* Coming to task 4, to get the latest ranks for any given pickup zones / pickup boroughs, you can query the `latest_zone_ranks` and `latest_borough_ranks` views in the database using a simple select query
* Steps required in order to add further information to the history table for:
   * For longest/shortest trip, average tip: a simple [max/min, avg] aggregations can be performed on the master_rides_fm while calculating the passenger counts. And the data can stored accordingly in the database table.
   * If any filters are to be placed on the groups, then we need to precompute the condition and then store the updated subgroups accordingly in the database.
* In order to create daily trends, we basically need to group the data by date and then apply the aggregations accordingly, and for yearly trends we basically group together the monthly data exports for each year and then do the transformations accordingly.

## Further possible enhancements
* Data export retrieval could be moved to S3 and could be automated even further using triggers on the website.
* The way the ETL pipeline is designed right now, once you have added the data for a month, you can't add the data for a month in the past. If we do a call on such an export then resulting ranking would be skewed since the ranks to be compared are fetched from the view, which has the latest data for ranks. Support for backfilling can be added using datetime partitioning when fetching the data for comparison.
* The complete pipeline can be moved to airflow for better overview of the dependencies and cleaner orchestration.

   
   




