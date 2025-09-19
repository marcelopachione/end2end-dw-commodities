# %%
# Import libs
import os
from extract import fetch_ticker, concat_tickers
from dbconfig import connect_to_database, create_schema
from load import save_to_db
from dotenv import load_dotenv

# Load envs
load_dotenv()

def main():
    commodities = ['CL=F', 'GC=F', 'SI=F']
    
    conn = connect_to_database()
    create_schema(conn)
    concatenated_tickers = concat_tickers(commodities)
    tb_commodities = save_to_db(concatenated_tickers, 'commodities')

if __name__ == '__main__':
    main()