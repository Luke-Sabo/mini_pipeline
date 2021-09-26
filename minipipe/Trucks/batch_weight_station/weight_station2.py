import pandas as pd
import numpy as np
import sqlite3
from datetime import date


###GOAL: load and process data from dozer to truck
source_connection = sqlite3.connect("weight_station.db")
source_cursor = source_connection.cursor()

political_users = pd.read_sql('SELECT * FROM Reddit_Political_Users', source_connection)

source_cursor.close()
source_connection.close()

print('\nPRE CLEAN\n')
print(political_users.info())
print(political_users.describe())

###Handling is_oc missread
#this is a sloppy and slow solution that produces a warning, 
#but works (and is faster than first attempt at least.)
oc_links = ['reddit.com', 'i.redd.it', 'v.redd.it', '/r/']
for idx, user in political_users.iterrows():
    #when importing the frame, all is_oc values were set to 0 -> false
    if user['is_oc'] == False:
        curr_url = user['url'][:25]
        political_users['is_oc'][idx] = False
        for link in oc_links:
            if link in curr_url:
                #print('found oc', user['url'])
                political_users['is_oc'][idx] = True

###sort by sub and author & reindex
#political_users['sub'] = political_users['sub'].str.lower()
#political_users = political_users.sort_values(by='sub', ascending = True)
political_users = political_users.sort_values(by='author', ascending = True)

political_users = political_users.reset_index(drop=True)

#making sure OC is correct values
#print(political_users['is_oc'].head(50), ' : ', political_users['url'].head(50))

###ENSURE types are casted correctly for db storage/access
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

print('\nPOST CLEAN\n')
print(political_users.info())
print(political_users.describe())

backup_path = "/media/pi/TRAVELDRIVE/truck_back_up"
scrape_date = str(date.today())
political_users.to_csv(backup_path + 'praw_politics_cleaned_stage2' + scrape_date + '.csv', index=False)

####PUSH CHANGES TO FOUNDRY LAKE
db_path = "/home/pi/minipipe/Cranes/foundry_lake/"
curr_db = db_path + 'praw_political_posts_cleaned2.db'
connection = sqlite3.connect(curr_db)
cursor = connection.cursor()
political_users.to_sql('Reddit_Political_Users',connection, if_exists = 'append', index=False)
connection.commit()
cursor.close()
connection.close()


