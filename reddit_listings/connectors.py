import logging
from typing import Union, Dict, Any, Tuple, Optional, List, Type
import sqlalchemy
import psycopg2

class PostgreSQLConnector():
    def __init__(self, database: str ='postgresql', connector: str ='psycopg2', **conn_parameters: Dict[Any, str]) -> None:
        """PostgreSQLConnector class

        Args:
            database (str): Database type i.e. 'postgresql'
            connector (str): Connector type i.e. 'psycopg2'
            conn_parameters (Dict[str]): Dictionary of required connection parameters

        Returns:
            none
        """
        self.__database = database
        self.__connector = connector
        self.__conn_parameters = conn_parameters
        self.__logger = logging.getLogger(self.__class__.__name__)
        print('PostgreSQLConnector Initializing...')

    def test_connection(self) -> None:
        """Method for testing PostgreSQL database connection 
        
        Args:
            none
        
        Returns:
            none
        """
        conn = None
        try: 
            self.__logger.debug('Connecting to PostgreSQL database...')
            print('Connecting to PostgreSQL database...')
            conn = psycopg2.connect(host=self.__conn_parameters['host'], 
                                    port=self.__conn_parameters['port'],
                                    database=self.__conn_parameters['db'],
                                    user=self.__conn_parameters['uid'],
                                    password=self.__conn_parameters['pwd'])

            cur = conn.cursor()
            self.__logger.debug('Checking PostgreSQL database version...')
            print('PostgreSQL database version:')
            cur.execute("SELECT version()")
            version = cur.fetchone()
            print(version)
            cur.close()
        except Exception as error:
            print('Cause: {}'.format(error))
        finally:
            if conn is not None:
                conn.close()
                self.__logger.debug('Database connection closed.')
                print('Database connection closed.')
    
    def connect(self):
        self.__logger.debug('Entering database connection...')
        print('Establishing Connection...')
        try:
            return sqlalchemy.create_engine(f"{self.__database}+{self.__connector}://{self.__conn_parameters['uid']}:{self.__conn_parameters['pwd']}@{self.__conn_parameters['host']}:{self.__conn_parameters['port']}/{self.__conn_parameters['db']}") 
        except Exception as e:
            self.__logger.error(
                f'Unable to create SQLAlchemy engine with error:\n{e}\nReturning None object', exc_info=True)
            return

    def create_table(self):
        db = sqlalchemy.create_engine(f"{self.__database}+{self.__connector}://{self.__conn_parameters['uid']}:{self.__conn_parameters['pwd']}@{self.__conn_parameters['host']}:{self.__conn_parameters['port']}/{self.__conn_parameters['db']}") 
        db.execute("CREATE TABLE IF NOT EXISTS realtors (name text)")
        db.execute("INSERT INTO realtors (name) VALUES ('Curt Strab')")
        db.execute("SELECT * FROM realtors")
