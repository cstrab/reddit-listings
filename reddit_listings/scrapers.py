from abc import ABC
import praw
import logging
from typing import Union, Dict, Any, Tuple, Optional, List, Type

class AbstractRedditScraper(ABC):
    """
    AbstractRedditScraper class with various convenience functions.
    Utilizes praw package.
    """
    def __init__(self, database, subreddit: str, **reddit_parameters: Dict[Any, str]):
        """Must reimplement for each new child class to specify subreddit

        Example:
        ChildScraper(AbstractRedditScraper):
            def __init__(subreddit, connection_parameters):
                super().__init__(subreddit, connection_parameters)

        Args:
            database (class object): Database connection object from connectors.py
            subreddit (str): i.e. 'appleswap'
            reddit_parameters (Dict[str]): Dictionary of required reddit parameters

        Returns:
            none
        """
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__logger.info('Initializing...')
        self.__database = database
        self.__client_id = reddit_parameters['client_id']
        self.__client_secret = reddit_parameters['client_secret']
        self.__user_agent = f"script:{reddit_parameters['name']}:{reddit_parameters['version']} (by /u/{reddit_parameters['reddit_username']})"  # <platform>:<app ID>:<version string> (by /u/<reddit username>)
        self.__logger.info('Initialization complete.')
        self.reddit = praw.Reddit(
            client_id=self.__client_id,
            client_secret=self.__client_secret,
            user_agent=self.__user_agent
        )
        self.subreddit = self.reddit.subreddit(subreddit)

class AppleSwapScraper(AbstractRedditScraper):
    def __init__(self, database, subreddit: str, **reddit_parameters: Dict[Any, str]):
        super().__init__(database, subreddit='appleswap', **reddit_parameters)
        self.__logger = logging.getLogger(self.__class__.__name__)
        print('Scraper')




