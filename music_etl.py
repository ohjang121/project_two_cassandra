# Import Python packages 
import pandas as pd
import cassandra
from cassandra.cluster import Cluster
import re
import os
import glob
import numpy as np
import json
import csv
import logging

### Logging Handling ###

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    logger.addHandler(sh)

def conso_data(data_directory):
    '''Reads the datasets in csv and consolidates it into one csv with only relevant columns
    Creates one csv as an output
    '''
    
    # Get your current folder and subfolder event_data
    filepath = os.getcwd() + data_directory
    
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        logger.info(f'list of all data files: {file_path_list}')

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = list()
    
    # for every filepath in the file path list 
    for f in file_path_list:
        # reading csv file
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile) # creating a csv reader object
            next(csvreader) # skip header
            # extracting each data row one by one and append it
            for line in csvreader:
                full_data_rows_list.append(line)

    # log total row count from the dataset
    logger.info(f'Total row count from the dataset: {len(full_data_rows_list)}') 
    
    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        # all desired columns for our output tables
        writer.writerow(['artist','first_name','gender','item_in_session','last_name','length','level','location','session_id','song','user_id'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    # check the number of rows in your csv file
    with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
        logger.info(f'Total row count from event_datafile_new.csv: {sum(1 for line in f)}')

def create_sparkifydb():
    '''Establish Cassandra connection and create sparkifydb keyspace
    Returns cluster and session object
    '''
    
    # connect to Cassandra locally
    try:
        cluster = Cluster(['127.0.0.1']) #if you have a locally installed Apache Cassandra Instance; can just do Cluster()
        session = cluster.connect()
        logger.info('Successful Cluster set up!')
    except Exception as e:
        logger.error(e)

    # create sparkifydb keyspace and set it for the working session
    try:
        session.execute('''
        create keyspace if not exists sparkifydb
        with replication =
        {'class':'SimpleStrategy', 'replication_factor':1}
        '''
        )
        session.set_keyspace('sparkifydb')
        logger.info('Successfully created and set current session to sparkifydb keyspace')
    except Exception as e:
        logger.error(e)

    return cluster, session

def create_table_1(session, file):
    '''Create music_app_history table based on the desired query 1
    '''
    ## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

    query_1 = '''SELECT artist, song, length
                 FROM music_app_history
                 WHERE session_id = 338
                 AND item_in_session = 4
    '''

    # model music_app_history based on query_1
    # set session_key = partition key (first restriction)
    # set item_in_session = clustering key (second restriction)
    create_query_1 = '''CREATE TABLE IF NOT EXISTS music_app_history
                        (session_id INT,
                        item_in_session INT,
                        artist VARCHAR,
                        song VARCHAR,
                        length FLOAT,
                        PRIMARY KEY (session_id, item_in_session)
    )'''

    # create music_app_history table
    try:
        session.execute(create_query_1)
    except Exception as e:
        logger.error(e)

    # insert data into music_app_history
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            ## Assign the INSERT statements into the insert_query_1 variable
            insert_query1 = "INSERT INTO music_app_history (session_id, item_in_session, artist, song, length)"
            insert_query1 += "VALUES (%s, %s, %s, %s, %s)"
            session.execute(insert_query1, (int(line[8]), int(line[3]), str(line[0]), str(line[9]), float(line[5])))

    # confirm data insertion in music_app_history
    try:
        rows = session.execute(query_1)
    except Exception as e:
        logger.error(e)
    # log query_1 output
    logger.info('''Query 1:  Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4''')
    for row in rows:
        print(row.artist, row.song, row.length)

def create_table_2(session, file):
    '''Create user_app_history table based on the desired query 2
    '''
    ## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

    query_2 = '''SELECT artist, song, first_name, last_name
             FROM user_app_history
             WHERE user_id = 10
             AND session_id = 182
    '''

    # model user_app_history based on query_2
    # set user_id & session_id = composite partition key as they are filtered in the where clause
    # set item_in_session = clustering key for sorting
    create_query_2 = '''CREATE TABLE IF NOT EXISTS user_app_history
                        (user_id INT,
                        session_id INT,
                        item_in_session INT,
                        artist VARCHAR,
                        song VARCHAR,
                        first_name VARCHAR,
                        last_name VARCHAR,
                        PRIMARY KEY ((user_id, session_id), item_in_session)
    )
    WITH CLUSTERING ORDER BY (item_in_session ASC)
    '''

    # create user_app_history table
    try:
        session.execute(create_query_2)
    except Exception as e:
        logger.error(e)

    # insert data into user_app_history
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            ## Assign the INSERT statements into the insert_query_2 variable
            insert_query2 = "INSERT INTO user_app_history (user_id, session_id, item_in_session, artist, song, first_name, last_name)"
            insert_query2 += "VALUES (%s, %s, %s, %s, %s, %s, %s)"
            session.execute(insert_query2, (int(line[10]), int(line[8]), int(line[3]), str(line[0]), str(line[9]), str(line[1]), str(line[4])))

    # confirm data insertion in user_app_history
    try:
        rows = session.execute(query_2)
    except Exception as e:
        logger.error(e)
    # log query_2 output
    logger.info('''Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182''')
    for row in rows:
        print(row.artist, row.song, row.first_name, row.last_name) # returned 4 rows, confirmed that it's in ascending order by item_in_session

def create_table_3(session, file):
    '''Create song_app_history table based on the desired query 3
    '''
    ## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own' 

    query_3 = '''SELECT first_name, last_name
                 FROM song_app_history
                 WHERE song = 'All Hands Against His Own'
    '''

    # model user_app_history based on query_3
    # set song = partition key as it is filtered in the where clause
    # set user_id = clustering key for sorting, and necessary to bring in to display every user
    create_query_3 = '''CREATE TABLE IF NOT EXISTS song_app_history
                        (song VARCHAR,
                        user_id INT,
                        first_name VARCHAR,
                        last_name VARCHAR,
                        PRIMARY KEY (song, user_id)
    )
    WITH CLUSTERING ORDER BY (user_id ASC) -- cluster key user_id to ensure we display all users, not just one
    '''

    # create song_app_history table
    try:
        session.execute(create_query_3)
    except Exception as e:
        logger.error(e)

    # insert data into song_app_history
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            ## Assign the INSERT statements into the insert_query_2 variable
            insert_query3 = "INSERT INTO song_app_history (song, user_id, first_name, last_name)"
            insert_query3 += "VALUES (%s, %s, %s, %s)"
            session.execute(insert_query3, (str(line[9]), int(line[10]), str(line[1]), str(line[4])))

    # confirm data insertion in song_app_history
    try:
        rows = session.execute(query_3)
    except Exception as e:
        logger.error(e)
    # log query_3 output
    logger.info('''Query 3: Give me every user name (first and last) in my music app history who listened to the song All Hands Against His Own''')
    for row in rows:
        print(row.first_name, row.last_name) 

def drop_tables(cluster, session):
    '''Drop all 3 tables and close the session
    '''
    drop_query_1 = 'DROP TABLE IF EXISTS music_app_history'
    drop_query_2 = 'DROP TABLE IF EXISTS user_app_history'
    drop_query_3 = 'DROP TABLE IF EXISTS song_app_history'

    try:
        session.execute(drop_query_3)
        logger.info('dropped all 3 tables successfully')
    except Exception as e:
        logger.error(e)

    logger.info('closing Cassandra connection, goodbye!')
    session.shutdown()
    cluster.shutdown()


def main():
    # set up necessary variables
    data_directory = '/event_data'
    file = 'event_datafile_new.csv'

    # consolidate data into one csv
    conso_data(data_directory)

    # set up cluster & session variable after creating sparkifydb keyspace
    cluster, session = create_sparkifydb()

    # create 3 tables based on the desired queries
    create_table_1(session, file)
    create_table_2(session, file)
    create_table_3(session, file)

    # drop tables and close connection once finished
    drop_tables(cluster, session)

if __name__ == '__main__':
    main()
