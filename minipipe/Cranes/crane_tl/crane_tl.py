import sys
import mariadb
import pandas as pd
import boto3
from io import StringIO

from db_credentials import *

###EXTRACT: Reddit political post information from mysql db
###TRANSFORM: with pandas, prune unnecessary info for this use case, sort, reindex
###LOAD: to s3 bucket for analysis 

###connect to lake server
try:
    print('attempting to connect...\n')
    crane_connection = mariadb.connect(
        user = user,
        password = password,
        host = local_host,
        database = 'praw_political_posts'
        )
    print('Success!\n')
    
except mariadb.Error as e:
    print(f"An error occured while connecting to Mariadb: {e}")
    sys.exit(1)

crane_cursor = crane_connection.cursor()

try:
    #crane_cursor.execute(" SELECT * FROM posts ")
    #user_frame = pd.DataFrame(crane_cursor.fetchall())
    post_frame = pd.read_sql("SELECT * FROM posts", crane_connection)
except mariadb.Error as e:
    print(f"An error occured while selecting from Mariadb: {e}")


print(post_frame)
###print(post_frame['sub'])
posts = post_frame.drop(['title', 'num_comments', 'timestamp', 'is_oc', 'url'], axis=1)
#print(posts)
###combine multiple post to the same sub by the same user by summing score
posts = posts.sort_values(by = ['author'])
combine = posts.groupby(by = ['author', 'sub'])['upvotes'].sum().reset_index()
sort_subs = combine.sort_values(by = ['sub'])
cleaned_posts = pd.concat(g for _, g in sort_subs.groupby('author') if len(g) > 1)
cleaned_posts = cleaned_posts[cleaned_posts.duplicated('author', keep=False)].groupby('author')['sub'].apply(list).reset_index()
print(cleaned_posts['author'].head(25))
print(cleaned_posts['sub'].head(25))
print(cleaned_posts.info())
print(cleaned_posts.describe())


bucket = 'reddit-political-posts-by-sub-user'
csv_buffer = StringIO()
cleaned_posts.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'cleaned_posts.csv').put(Body=csv_buffer.getvalue())


