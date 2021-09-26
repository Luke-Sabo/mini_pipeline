import sys
import mariadb
import pandas as pd
import sqlite3
from db_credentials import *

###pull data to push to lake
lite_connection = sqlite3.connect("/home/pi/minipipe/Cranes/foundry_lake/praw_political_posts_cleaned2.db")

political_users = pd.read_sql('SELECT * FROM Reddit_Political_Users', lite_connection)

lite_connection.close()
print(political_users)




###connect to lake server
try:
    print('attempting to connect...\n')
    lake_connection = mariadb.connect(
        user = user,
        password = password,
        host = local_host,
        database = 'praw_political_posts'
        )
    print('Success!\n')
    
except mariadb.Error as e:
    print(f"An error occured while connecting to Mariadb: {e}")
    sys.exit(1)

lake_cursor = lake_connection.cursor()
#push to lake server
##pandas to_sql i believe assumes a sqlite connection, I am getting errors trying to push this frame to mariadb
lake_cursor.execute("CREATE TABLE posts( pid VARCHAR(6), sub VARCHAR(21), title LONGTEXT, author VARCHAR(21), upvotes INT, upvote_ratio FLOAT, num_comments INT, timestamp VARCHAR(20), is_oc BOOL, url VARCHAR(2083))" ) 
try:
    for idx, user in political_users.iterrows():
        pid = user['pid']
        sub = user['sub']
        title = user['title']
        author = user['author']
        upvotes = user['upvotes']
        upvote_ratio = user['upvote_ratio']
        num_comments = user['num_comments']
        timestamp = user['timestamp']
        is_oc = user['is_oc']
        url = user['url']

        lake_cursor.execute(
            "INSERT INTO posts (pid, sub, title, author, upvotes, upvote_ratio, num_comments, timestamp, is_oc, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (pid, sub, title, author, upvotes, upvote_ratio, num_comments, timestamp, is_oc, url))
        
        lake_connection.commit()

except mariadb.Error as e:
    print(f"An error occured while manipulating table: {e}")


