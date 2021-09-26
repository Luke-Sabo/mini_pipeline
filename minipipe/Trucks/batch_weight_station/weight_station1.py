import pandas as pd
import numpy as np
import sqlite3

###GOAL: load and process data from dozer to truck
source_connection = sqlite3.connect("/home/pi/minipipe/Dozers/praw_politics_lm1000.db")
source_cursor = source_connection.cursor()

political_users = pd.read_sql('SELECT * FROM Reddit_Political_Users', source_connection)

source_cursor.close()
source_connection.close()


print(political_users.info())
print(political_users.describe())
#print(political_users['author'].head(50))
#by default: df.dropna(axis='index', how='any')
political_users = political_users.dropna()
print(political_users)

###With our current goal/desired data set, we really need authors
###remove entries w/out valid user name (author col) 
###currently, missing author values are stored as 'None' type obj,
###we will replace with np.nan vals to drop in one line
political_users = political_users.replace(to_replace='None', value=np.nan).dropna()
print(political_users)


###Recast all dtypes to ensure they are written correctly to db
###and further manipulation is valid
political_users['pid'] = political_users['pid'].astype(str)
political_users['sub'] = political_users['sub'].astype(str)
political_users['title'] = political_users['title'].astype(str)
political_users['author'] = political_users['author'].astype(str)
political_users['upvotes'] = political_users['upvotes'].astype(int)
political_users['upvote_ratio'] = political_users['upvote_ratio'].astype(float)
political_users['num_comments'] = political_users['num_comments'].astype(int)
political_users['timestamp'] = political_users['timestamp'].astype(str)
###Note: sqlite3 stores bools as integers, 0 or 1
political_users['is_oc'] = political_users['is_oc'].astype(bool)
political_users['url'] = political_users['url'].astype(str)


###write changes to new db
###Weight station db is meant to serve as a staging area
###between the two cleaning phases
target_connection = sqlite3.connect(str(date.today()) + "_weight_station.db")
target_cursor = target_connection.cursor()

political_users.to_sql('Reddit_Political_Users',target_connection, if_exists = 'append', index=False)
target_connection.commit()
target_cursor.close()
target_connection.close()