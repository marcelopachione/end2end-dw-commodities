# %%
import os
import pandas as pd
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load envs
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_db_engine():
    """
    Create and return a SQLAlchemy engine for the PostgreSQL database.
    """
    db_url = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )
    return create_engine(db_url, echo=False)

# %%
def save_to_db(df, table_name):
    """
    Save a DataFrame to database.

    Args:
        df: The DataFrame to be saved.
        schema: The target database schema.
    """
    logger.info("Starting data insertion into the 'commodities' table.")
    try:
        engine = create_db_engine()

        with engine.begin() as conn:
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists='append',
                index=True,
                index_label='Date',
                schema=os.getenv('POSTGRES_SCHEMA')
            )
        logger.info("Data successfully inserted.")
    except Exception as e:
        logger.error("Error inserting data into the database: %s", e)
        raise