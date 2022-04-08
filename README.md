# Project 2: Data Modeling with Cassandra

Objective: Build an ETL pipeline for a music app (Sparkify) using Python & Cassandra. Main dataset involved lives in `event_data` directory.

## Solution Approach

I compiled all logic into one script, [music_etl.py](https://github.com/ohjang121/project_two_cassandra/blob/master/music_etl.py), derived from the Jupyter Notebook step by step guidance. This script achieves the same purpose with log messages helpful for the reviewer to understand each step.

1. `conso_data()`: Consolidate log data in the `event_data` directory into 1 csv, write it into `event_datafile_new.csv` (done by default)
2. `create_sparkifydb()`: Establish Cassandra connection and create sparkifydb keyspace; returns cluster and session object
3. `create_table_1()`: Create music_app_history table based on the desired query 1
4. `create_table_2()`: Create user_app_history table based on the desired query 2
5. `create_table_3()`: Create song_app_history table based on the desired query 3
6. `drop_tables()`: Drop all 3 tables and close the session
