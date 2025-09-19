# Import libs
import os
import logging
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from dbconfig import connect_to_database, create_schema

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_ticker(ticker, period='5d', interval='1d'):
    """
    Fetch the historical prices for a ticker from yfinance.

    Returns
    -------
    pd.DataFrame
        Columns: ['Close', 'ticker'].
        Returns an empty DataFrame with those columns if no data
        is available or if an error occurs.
    """
    try:
        hist = yf.Ticker(ticker).history(period=period, interval=interval)

        if hist.empty:
            logger.info("No data for %s (period=%s, interval=%s).", ticker, period, interval)
            return pd.DataFrame(columns=["Close", "ticker"])

        df = hist[["Close"]].copy()

        df["ticker"] = ticker
        logger.info("=======================================")
        logger.info("Fetching ticker ==> %s.", ticker)
        logger.info("Fetched %s (%d rows).", ticker, len(df))
        logger.info("=======================================")
        return df
    except Exception:
        logger.exception("Error fetching %s (period=%s, interval=%s).", ticker, period, interval)
        return pd.DataFrame(columns=["Close", "ticker"])


def concat_tickers(commodities):
    """
    Fetch and combine historical data for multiple tickers.

    Parameters
    ----------
    commodities
        List (or other iterable) of ticker commodities.

    Returns
    -------
    pd.DataFrame
        Concatenated DataFrame of all commodities' data.
        Empty DataFrame if no data is retrieved.
    """

    frames = []

    for symbol in commodities:
        df = fetch_ticker(symbol)
        if not df.empty:       
            frames.append(df)

    if not frames:            
        return pd.DataFrame(columns=["Close", "ticker"])

    return pd.concat(frames, ignore_index=False)