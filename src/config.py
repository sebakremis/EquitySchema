# src/config.py
from pathlib import Path

# Data sources
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
all_tickers_file = DATA_DIR / 'all_tickers.csv'
stocks_folder = DATA_DIR / 'stocks'
dim_ticker_file = stocks_folder / 'dim_ticker.csv'
prices_log_file = stocks_folder / 'prices_log.json'