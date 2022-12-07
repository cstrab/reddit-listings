import os
from dotenv import load_dotenv
from reddit_listings.connectors import PostgreSQLConnector
from reddit_listings.scrapers import AppleSwapScraper


if __name__ == "__main__":
    load_dotenv()

    conn_params = dict(
        uid=os.environ['UID'],
        pwd=os.environ['PW'],
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

    # Intializing connector and scraper
    connector = PostgreSQLConnector(**conn_params)
    scraper = AppleSwapScraper(connector=connector, subreddit='appleswap', **reddit_params)

    # Inital Data Pull (Last 1000 posts on r/appleswap)
    new_posts = scraper.subreddit.new(limit=1000)
    query = """INSERT INTO posts (id, title, body, author, created) 
            VALUES (%s, %s, %s, %s, %s);"""
    print('Scraping Initiated <(x.x)>')
    for post in new_posts:
        scraper.engine.execute(query, (
            post.id,
            post.title,
            post.selftext,
            post.author.name if post.author else "",
            post.created,
        ))
    print('Scraping Complete <(-.-)<')


