import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
import importlib
from scenario_logger import logger  # Importing the logger
import json

class SQLQueryExecutor:
    def __init__(self):
        # Using the imported logger
        self.logger = logger

        # Create a SQLAlchemy engine for database connection
        self.engine = self.create_engine()

    def load_sql_query(self, file_path):
        with open(file_path, 'r') as file:
            data = file.read()
        return data

    def load_config_from_json(self, config_file_path):
        with open(config_file_path, 'r') as config_file:
            config = json.load(config_file)
        return config

    def create_engine(self):
        """
        Create a SQLAlchemy engine with connection pooling.

        Returns:
        - SQLAlchemy Engine: An engine object for database connection.
        """
        try:
            conn_str = URL.create(
                drivername="mssql+pyodbc",
                username='script_user',
                password='transvision@2024',
                host='34.196.144.75',
                database='TS_STAR',
                query={"driver": "ODBC Driver 17 for SQL Server", "timeout": "300"}
            )
            # conn_str = URL.create(
            #     drivername="mssql+pyodbc",
            #     username='transvision',
            #     password='Transvision@2023',
            #     host='54.226.161.24',
            #     database='TS_STAR',
            #     query={"driver": "ODBC Driver 17 for SQL Server", "timeout": "300"}
            # )

            # Use connection pooling with appropriate pool size and timeout settings
            self.logger.info("Creating SQLAlchemy engine for database connection.")
            return create_engine(conn_str, pool_size=5, max_overflow=10, pool_timeout=30, pool_recycle=1800)
        except Exception as e:
            self.logger.error(f"Error creating SQLAlchemy engine: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        Context manager for providing a database connection.
        Ensures proper cleanup of connections.
        """
        connection = None
        try:
            connection = self.engine.connect()
            yield connection
        except SQLAlchemyError as e:
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()

    def execute_query(self, sql_query):
        """
        Execute a given SQL query and return the result as a Pandas DataFrame.

        Parameters:
        - sql_query (str): The SQL query to be executed.

        Returns:
        - Pandas DataFrame: Result of the SQL query.
        """

        try:
            # Use the context manager for database connection
            with self.get_connection() as connection:
                if sql_query.strip().lower().startswith('select'):
                    result = pd.read_sql_query(text(sql_query), connection)
                    return result
                else:
                    with connection.begin() as trans:
                        connection.execute(text(sql_query))
                        trans.commit()
        except SQLAlchemyError as e:
            # Log an error message if the query execution fails
            self.logger.error(f"SQLAlchemy Error executing SQL query - {sql_query} | {e}")
        except Exception as e:
            # Log a general error message if other exceptions occur
            self.logger.error(f"Error executing SQL query - {sql_query} | {e}")

        # Return an empty DataFrame in case of an error
        return pd.DataFrame()