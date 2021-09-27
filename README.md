# MINI PIPELINE

Mini pipe is a data pipeline I designed and implemented to familiarize myself with some new tools and concepts. 
All code was written and ran on a Raspberry pi 4, accessed through ssh.

# Dependencies 

- AWS CLI
- boto3
- Mariadb
- mysql-connector
- Pandas
- PRAW (Python Reddit API Wrapper)
- SQLite3

## Pipeline Structure

With the idea of 'mining' in mind...\
**Dozers** - where scripts and streams are stored to scrape and pull information from the web, the first step in the pipeline, backing up the initial scrape and creating a sqlite3 database to take the data to the next step.\
**Trucks** - The Truck folder is where the data is carried between 'weigh stations'. In these weight station files the data is taken through a standard data cleaning process: Handling null values, missing values, inconsistent or inaccurate data, formatting, etc. The data is stored between two cleaning stages in a different sqlite3 database, once the data set has moved through the second stage, it is pushed to the data lake, my mysql server running on my Raspberry Pi, using Mariadb as my RDMS. \
**Cranes** - Utilizing a user I created with limited SELECT privileges to my mysql server, cranes pull data from the database and take it through a project-specific ETL process, in this case finding influential sub-communities within the political space of Reddit. Once the data has been transformed to the required set, it is finally pushed to an AWS S3 bucket for further access and analysis. \
  

## Pipeline Design goals
