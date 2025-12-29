"""
src.core.py
Core functionalities for EquitySchema application.
"""
import pandas as pd
import yfinance as yf
from pathlib import Path
from src.config import all_tickers_file

# --- Tickers management functions ---

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
    
    # Tickers validation
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
    """
    # Check which tickers are actually present
    existing_tickers = tickers_df["Ticker"].tolist()
    for ticker in tickers_to_remove:
        if ticker not in existing_tickers:
            print(f"Ticker '{ticker}' not found in the database. Skipping removal.")

    updated_tickers_df = tickers_df[~tickers_df["Ticker"].isin(tickers_to_remove)].reset_index(drop=True)
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