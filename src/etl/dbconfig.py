import os
import logging
import psycopg2 
from psycopg2 import sql
from dotenv import load_dotenv

# Load envs
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## Create DB if not exist
def create_db(database_name):
    logger.info(f"Checking if database {os.getenv('POSTGRES_DB')} exists")

    try:
        conn = psycopg2.connect(
            dbname=os.getenv('postgres'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=5432,
        )

        try:
            conn.set_session(autocommit=True)
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
                if cursor.fetchone() is None:
                    # cursor.execute("CREATE DATABASE {database_name};")
                    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
                    logger.info(f"Database {database_name} created.")        
        finally:
            conn.close()
    except psycopg2.Error as e:
        logger.error(f"Failed to create database {database_name}, error : {e}")
        return None


## DB Connection
def connect_to_database():

    create_db(os.getenv('POSTGRES_DB'))

    logger.info(f"Connecting to the database {os.getenv('POSTGRES_DB')}")

    try:
        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=5432,
        )

        logger.info(f"Sucessfully connect to the database {os.getenv('POSTGRES_DB')}")

        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connec to the database {os.getenv('POSTGRES_DB')}, error : {e}")
        return None


## Create schema
def create_schema(conn):
    
    if not conn:
        logging.error("No database connection available.")
        return None
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS dw_comm;")
            conn.commit()

            logger.info("Schema and table created sucessfully.")
    except psycopg2.Error as e:
        logging.error(f"Error creating schema and table: {e}.")
        return None    