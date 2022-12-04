# Abstract and logging
from abc import ABC 
import logging
from typing import Union, Dict, Any, Tuple, Optional, List, Type

# SQLAlchemy
import sqlalchemy
from sqlalchemy.pool import StaticPool

# Connection to PostgreSQL database
import psycopg2

class AbstractConnector(ABC):

    def __init__(self, database: str, connector: str, **connection_parameters: Dict[Any, str]):
        """Must reimplement for each new child class to specify database and connection type

        Example:
        ChildConnector(AbstractConnector):
            def __init__(database, connector, connection_parameters):
                super().__init__(database, connector, connection_parameters)

        Args:
            database (str): Database type: i.e. postgresql
            connector (str): Package/module used for making database connection: i.e. psycopg2
            connection_parameters (Dict[str]): Dictionary of required connection parameters
        
        Returns:
            none
        """
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__logger.info('Initializing...')
        self.__database = database
        self.__connector = connector
        self.__connection_parameters = connection_parameters
        self.__logger.info('Initialization complete.')

    def __enter__(self):
        """Creates SQLAlchemy database specific server engine based on connector
        Returns:
            Database connection object

        Raises:
            UnableToConnect
        """
        print('im entering beep boop')
        self.__logger.debug('Entering database connection...')
        try:
            return sqlalchemy.create_engine(f"{self.__database}+"
                                            f"{self.__connector}://+"
                                            f"{self.__connection_parameters['uid']}:+"
                                            f"{self.__connection_parameters['pwd']}+"
                                            f"@{self.__connection_parameters['host']}+"
                                            f"{self.__connection_parameters['port']}/+"
                                            f"{self.__connection_parameters['db']}")
        except Exception as e:
            self.__logger.error(
                f'Unable to create SQLAlchemy engine with error:\n{e}\nReturning None object', exc_info=True)
        return

    def __exit__(self):
        return

# https://docs.sqlalchemy.org/en/20/core/engines.html to add more connectors
class PostgreSQLConnector(AbstractConnector):
    def __init__(self, **connection_parameters: Dict[Any, str]):
        super().__init__(database='postgresql', connector='psycopg2', **connection_parameters)
        self.__logger = logging.getLogger(self.__class__.__name__)
        print('Connection')