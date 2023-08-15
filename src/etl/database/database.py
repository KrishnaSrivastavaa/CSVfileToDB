import psycopg2
from configparser import ConfigParser
import os
from src.etl.logger.logger import logger

class Database:
    def __init__(self):
        self.config = ConfigParser()
        self.config.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', '..', 'config', 'config.ini'))
        
    def connect(self):
        try:
            conn = psycopg2.connect(
                host=self.config.get('DATABASE', 'host'),
                database=self.config.get('DATABASE', 'database'),
                user=self.config.get('DATABASE', 'user'),
                password=self.config.get('DATABASE', 'password')
            )
            logger.info("Connected to the database")
            return conn
        except Exception as e:
            logger.error("Error connecting to the database: %s", e)
            raise

    def execute_query(self, conn, query, values=None):
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, values)
                conn.commit()
        except Exception as e:
            logger.error("Error executing query: %s", e)
            conn.rollback()
            raise
