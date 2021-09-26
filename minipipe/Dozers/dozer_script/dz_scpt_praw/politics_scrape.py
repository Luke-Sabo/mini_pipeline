import numpy as np
import pandas as pd
from datetime import datetime, date
import sqlite3
import praw
#local imports
from subs_politics import *
from config import *

###file patch for csv storage -> /media/pi/TRAVELDRIVE/dozer_batch

reddit = praw.Reddit(client_id = client_id, client_secret = client_secret, \
                     user_name = user_name, password = password, user_agent = user_agent)


user = { 'pid':[],'sub':[], 'title':[], 'author':[], 'upvotes':[], 'upvote_ratio':[], 'num_comments':[],'timestamp':[], 'is_oc':[], 'url':[]}
for sub in subs_politics:
    try:
        print('currently scraping...r/', sub)
        print('_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _')
        
        subreddit = reddit.subreddit(sub)
        top_posts = subreddit.top("all", limit=1000)
        
        for post in top_posts:
            user['pid'].append(post.id)
            user['sub'].append(sub)
            user['title'].append(post.title)
            curr_author = str(post.author)
            print('scraping user... ', curr_author)
            user['author'].append(curr_author)
            user['upvotes'].append(post.score)
            user['upvote_ratio'].append(post.upvote_ratio)
            user['num_comments'].append(post.num_comments)
            ##Convert unix time to something readable to humans##
            unix_time = post.created_utc
            curr_timestamp = str(datetime.fromtimestamp(unix_time))
            #####################################################
            user['timestamp'].append(curr_timestamp)
            user['is_oc'].append(post.is_original_content)
            user['url'].append(post.url)
            print('____________________________________')
    except Exception as e:
        print(e)



user_frame = pd.DataFrame(user)
print(user_frame)
###CSV EXPORT
CSV_path = '/media/pi/TRAVELDRIVE/dozer_batch/'
scrape_date = str(date.today())
user_frame.to_csv(CSV_path + 'praw_politics_scrape_lm1000' + scrape_date + '.csv', index=False)

###SQLite export
###SQLite seems to not like taking pandas objects as input, need to recast

user_frame['pid'] = user_frame['pid'].astype(str)
user_frame['sub'] = user_frame['sub'].astype(str)
user_frame['title'] = user_frame['title'].astype(str)
user_frame['author'] = user_frame['author'].astype(str)
user_frame['upvotes'] = user_frame['upvotes'].astype(int)
user_frame['upvote_ratio'] = user_frame['upvote_ratio'].astype(float)
user_frame['num_comments'] = user_frame['num_comments'].astype(int)
user_frame['timestamp'] = user_frame['timestamp'].astype(str)
###Note: sqlite3 stores bools as integers, 0 or 1
user_frame['is_oc'] = user_frame['is_oc'].astype(int)
user_frame['url'] = user_frame['url'].astype(str)


################################
DB_path = '/home/pi/minipipe/Dozers/'
db = DB_path + 'praw_politics_lm1000.db'
connection = sqlite3.connect(db)
cursor = connection.cursor()
user_frame.to_sql('Reddit_Political_Users',connection, if_exists = 'replace', index=False)
connection.commit()
cursor.close()
connection.close()


