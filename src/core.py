"""
src.core.py
Core functionalities for EquitySchema application.
"""
import pandas as pd
import json
import yfinance as yf
from pathlib import Path
from src.config import (
    all_tickers_file, prices_log_file, stocks_folder,
    dim_ticker_file
)
# --- Ticker management functions ---

def load_tickers(tickers_path: Path = all_tickers_file) -> pd.DataFrame:
    """
    Loads tickers from a single CSV file (all_tickers_file by default).
    Returns a DataFrame with a 'Ticker' column.
    """   
    tickers_df = pd.DataFrame()

    try:
        tickers_df = pd.read_csv(all_tickers_file)
    except FileNotFoundError:
        print(f"Error loading tickers: File not found at {all_tickers_file}")
        return pd.DataFrame(columns=['Ticker'])
    except Exception as e:
        print(f"Error loading tickers: {e}")
        return pd.DataFrame(columns=['Ticker'])

    if tickers_df.empty or 'Ticker' not in tickers_df.columns:
        print(f"No tickers found in {all_tickers_file}.")
        return pd.DataFrame(columns=['Ticker'])
        
    return tickers_df

def add_tickers(new_tickers_str: str, tickers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds new tickers to the existing tickers DataFrame.
    Expects a string of tickers separated by spaces or commas.
    Returns the updated DataFrame.
    """
    new_tickers = [ticker.strip().upper() for ticker in new_tickers_str.replace(",", " ").split() if ticker.strip() and ticker.strip().upper() not in tickers_df["Ticker"].values]
    
    # Ticker validation
    valid_tickers = []
    for ticker in new_tickers:
        try:
            # if the ticker is invalid, the dataframe will be empty
            hist = yf.Ticker(ticker).history(period="1d")
            if not hist.empty:
                valid_tickers.append(ticker)
            else:
                print(f"Ticker '{ticker}' found but has no data (likely invalid/delisted).")    
        except Exception as e:
            print(f"Ticker '{ticker}' caused an error: {e}")
    
    if valid_tickers:
        updated_tickers_df = pd.concat([tickers_df, pd.DataFrame(valid_tickers, columns=["Ticker"])], ignore_index=True)
        return updated_tickers_df
    else:
        raise ValueError("No valid new tickers to add.")

def remove_tickers(tickers_df: pd.DataFrame, tickers_to_remove: list) -> pd.DataFrame:
    """
    Removes specified tickers from the existing tickers DataFrame.
    Expects a list of tickers to remove.
    Returns the updated DataFrame.
    Removes specified tickers from the dataframe, the JSON log, and deletes their data files.
    """
    # Check which tickers are actually present
    existing_tickers = tickers_df["Ticker"].tolist()
    valid_removals = []

    for ticker in tickers_to_remove:
        if ticker in existing_tickers:
            valid_removals.append(ticker)
        else:
            print(f"Ticker '{ticker}' not found in the existing tickers list. Skipping removal.")
    
    if not valid_removals:
        return tickers_df  # Nothing to remove

    updated_tickers_df = tickers_df[~tickers_df["Ticker"].isin(valid_removals)].reset_index(drop=True)

    # Remove from prices log file (JSON)
    if prices_log_file.exists():
        try:
            with open(prices_log_file, 'r') as f:
                prices_log = json.load(f)
            log_changed = False
            for ticker in valid_removals:
                if ticker in prices_log:
                    del prices_log[ticker]
                    log_changed = True

            if log_changed:
                with open(prices_log_file, 'w') as f:
                    json.dump(prices_log, f, indent=4)
                print(f"Removed {len(valid_removals)} ticker(s) from prices log.")
        except Exception as e:
            print(f"Error updating prices log: {e}")
            
    # Delete ticker data files in separated try-except blocks
    for ticker in valid_removals:
        # remove from metadata file
        try:
            metadata_file = dim_ticker_file
            if metadata_file.exists():
                metadata_df = pd.read_csv(metadata_file)
                if ticker in metadata_df['Ticker'].values:
                    metadata_df = metadata_df[metadata_df['Ticker'] != ticker]
                    metadata_df.to_csv(metadata_file, index=False)
                    print(f"Removed ticker {ticker} from metadata file.")
        except Exception as e:
            print(f"Could not delete metadata file for ticker {ticker}: {e}")

        # remove prices file
        try:
            price_file = stocks_folder / 'prices' / f"{ticker}.parquet"
            if price_file.exists():
                price_file.unlink()           
            print(f"Deleted price file for ticker {ticker}.")
        except Exception as e:
            print(f"Could not delete price file for ticker {ticker}: {e}")  

        # Remove financials file
        try:
            financials_file = stocks_folder / 'financials' / f"{ticker}.parquet"
            if financials_file.exists():
                financials_file.unlink()
        except Exception as e:
            print(f"Could not delete financials file for ticker {ticker}: {e}") 

    return updated_tickers_df

def save_tickers(tickers_df: pd.DataFrame, tickers_path: Path = all_tickers_file):
    """
    Saves tickers to a single CSV file (all_tickers_file by default).
    Expects a DataFrame with a 'Ticker' column.
    """
    try:
        tickers_df.to_csv(tickers_path, index=False)
    except Exception as e:
        print(f"Error saving tickers: {e}")

    