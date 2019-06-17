Project summary:
----------------
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project implements an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables that will be used by their analytics teams to discover insights into their users listening habits.



How it works:
-------------
Since the analytics team is priamrily intereted in discovering insights into the song play behaviours of their users, we implement a star schema with songplays at its centre as a fact table since it is the metric we wish to analyze and add four dimensional tables: songs, users, artists and time. This structure is optimized for queries on song play analysis.

dwh.cfg: the configuration file that contains the parameters needed to connect to the redshift cluster (host, data base details, iam_role, etc.. )

sql_queries.py: containts all the queries to to build the dimensional model, extract data from S3, load it into the staging tablesand insert the data into the dimenisonal model. 

create_tables.py: connects to redshift and builds a sparkify database if doesn't exists. Once the database is created, this fils executes the SQL queries in sql_queries.py to build tables in the schema.

etl.py: performs the ETL in two steps. The data is transfered from the the JSON files on S3 into the staging tables on redshift. The data is then transformed and loaded into the dimensional model.


How to use the scripts:
-----------------------

Prerequisite to run the code:
Since this code performs an ETL to load data onto a redshift cluster (from S3 buckets), it assumes that a redshift cluseter has been lauched (status available) and that an iam role has been created. The appropriate fielda in dwh.cfg should be updated to include your cluster information.

To build the schema(or to drop the tables and rebuild them)
python create_tables.py

To execute the ETL to load data into the tables:
python etl.py

