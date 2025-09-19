# Import libs
import os
import logging
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from extract import fetch_ticker, concat_tickers
from dbconfig import connect_to_database, create_schema

def main():
    conn = connect_to_database()
    create_schema(conn)
    commodities = ['CL=F', 'GC=F', 'SI=F']
    concatenated_tickers = concat_tickers(commodities)

if __name__ == '__main__':
    main()