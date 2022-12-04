import os
from dotenv import load_dotenv
from reddit_listings.connectors import PostgreSQLConnector
from reddit_listings.scrapers import AppleSwapScraper

load_dotenv()

# .env file should be at root level formatted as below:
# PYTHONUNBUFFERED=1
# UID=''
# PWD=''
# HOST=''
# PORT=''
# DATABASE=''
# CLIENT_ID=''
# CLIENT_SECRET=''
# NAME=''
# VERSION=''
# REDDIT_USERNAME=''

conn_params = dict(
    uid=os.environ['UID'],
    pwd=os.environ['PWD'],
    host=os.environ['HOST'],
    port=os.environ['PORT'],
    db=os.environ['DATABASE']
)

reddit_params = dict(
    client_id=os.environ['CLIENT_ID'],
    client_secret=os.environ['CLIENT_SECRET'],
    name=os.environ['NAME'],
    version=os.environ['VERSION'],
    reddit_username=os.environ['REDDIT_USERNAME']
)

database = PostgreSQLConnector(**conn_params)

scraper = AppleSwapScraper(database=database, subreddit='appleswap', **reddit_params)



# Test
new_submissions = scraper.subreddit.new(limit=1)

content = []

for submission in new_submissions:
    title = submission.title
    selftext = submission.selftext
    content.append(f'Title: {title}'+' '+f'Description: {selftext}')

print(content)


