# src/etl.py
import pandas as pd
import json
import yfinance as yf
from .config import (
    DATA_DIR, stocks_folder, dim_ticker_file,
    prices_log_file
    )
from src.core import load_tickers

# --- Helper Functions for Log ---
def load_prices_log() -> dict:
    """
    Loads the prices log and syncs it with the filesystem.
    If a parquet file is missing, the log entry is removed immediately.
    """
    if prices_log_file.exists():
        try:
            with open(prices_log_file, 'r') as f:
                log_data = json.load(f)
            
            # Sync logic
            prices_folder = stocks_folder / 'prices'
            clean_log = {}
            log_modified = False

            for ticker, date_str in log_data.items():
                file_path = prices_folder / f"{ticker}.parquet"
                
                # Only keep the entry if the file actually exists
                if file_path.exists():
                    clean_log[ticker] = date_str
                else:
                    log_modified = True # Mark for update
            
            # If we cleaned up any entries, save the file back to disk
            if log_modified:
                save_prices_log(clean_log)
                print(f"♻️  Synchronized prices log: Removed entries for missing files.")
                return clean_log

            return log_data
        except Exception as e:
            print(f"Error loading log: {e}")
            return {}
    return {}

def save_prices_log(log_dict: dict):
    with open(prices_log_file, 'w') as f:
        json.dump(log_dict, f, indent=4)

# --- Vectorized Cleaning Function ---

def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies data quality rules and anomaly detection to a combined DataFrame.
    Handles multiple tickers simultaneously using vectorized operations.
    """
    if df.empty:
        return df

    # Enforce numeric types for price and volume columns 
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'dividends', 'stockSplits']
    cols_present = [c for c in numeric_cols if c in df.columns]
    df[cols_present] = df[cols_present].apply(pd.to_numeric, errors='coerce')

    # Anomaly detection: Mask non-positive prices and negative volumes 
    price_cols = ['open', 'high', 'low', 'close']
    valid_prices = [c for c in price_cols if c in df.columns]
    if valid_prices:
        df[valid_prices] = df[valid_prices].mask(df[valid_prices] <= 0)
    
    if 'volume' in df.columns:
        df['volume'] = df['volume'].mask(df['volume'] < 0)
    
    # Ensure chronological order and forward fill within ticker groups to prevent data leakage 
    df = df.sort_values(['Ticker', 'Date'])
    df = df.groupby('Ticker', group_keys=False).apply(lambda x: x.ffill(limit=5))

    return df

# --- Data Extraction Functions ---
  
def fetch_prices(ticker: str, period: str = None, start: str = None, interval: str = '1d') -> pd.DataFrame:
    """Fetches raw data and renames columns for schema alignment."""
    try:
        yf_ticker = yf.Ticker(ticker)
        data = yf_ticker.history(period=period, start=start, interval=interval) if period or start else pd.DataFrame()
        
        if data.empty:
            return pd.DataFrame()

        data.rename(columns={
            'Open': 'open', 'High': 'high', 'Low': 'low',
            'Close': 'close', 'Volume': 'volume',
            'Dividends': 'dividends', 'Stock Splits': 'stockSplits'
        }, inplace=True)

        data['Ticker'] = ticker
        data.reset_index(inplace=True)
        # Ensure 'Date' is timezone-naive for Parquet compatibility 
        if data['Date'].dt.tz is not None:
            data['Date'] = data['Date'].dt.tz_localize(None)
            
        return data
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return pd.DataFrame()


def fetch_metadata(ticker: str) -> dict:
    """
    Extract metadata for a given ticker using yfinance.
    Returns a dictionary with relevant metadata fields.
    """
    metadata = {}
    try:
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info
        metadata = {
            'Ticker': ticker,
            'shortName': info.get('shortName', ''),
            'sector': info.get('sector', ''),
            'industry': info.get('industry', ''),
            'country': info.get('country', ''),
            'marketCap': info.get('marketCap', 0),
            'beta': info.get('beta', None),
            'dividendYield': info.get('dividendYield', None),
            '52WeekHigh': info.get('fiftyTwoWeekHigh', None),
            '52WeekLow': info.get('fiftyTwoWeekLow', None),
            # valuation data
            'forwardPE': info.get('forwardPE', None),
            'priceToBook': info.get('priceToBook', None),
            'enterpriseToEbitda': info.get('enterpriseToEbitda', None),
            # profitability data
            'returnOnAssets': info.get('returnOnAssets', None),
            # last METADATA update timestamp
            'lastUpdated': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')           
        }
        etfs_df = pd.read_csv(DATA_DIR/'etfs.csv')
        if ticker in etfs_df['Ticker'].values:
            metadata['sector'] = 'ETF'

        print(f"Metadata for {ticker} extracted successfully.")
        return metadata
    except Exception as e:
        print(f"Error extracting metadata for ticker {ticker}: {e}")
        return {}

def fetch_financials(ticker: str) -> pd.DataFrame:
    """
    Fetches Annual and Quarterly financials (Income Statement),
    transposes them for Power BI (Dates as rows), and adds Ticker/Type columns.
    """
    try:
        yf_ticker = yf.Ticker(ticker)
        
        # 1. Fetch Annual and Quarterly Data
        annual = yf_ticker.financials
        quarterly = yf_ticker.quarterly_financials
        
        if annual.empty and quarterly.empty:
            print(f"No financials found for {ticker}")
            return pd.DataFrame()

        dfs_to_concat = []

        # 2. Process Annual Data
        if not annual.empty:
            # Transpose: Switch Rows (Metrics) and Columns (Dates)
            annual_T = annual.T 
            annual_T['Ticker'] = ticker
            annual_T['PeriodType'] = 'Annual'
            dfs_to_concat.append(annual_T)

        # 3. Process Quarterly Data
        if not quarterly.empty:
            # Transpose
            quarterly_T = quarterly.T 
            quarterly_T['Ticker'] = ticker
            quarterly_T['PeriodType'] = 'Quarterly'
            dfs_to_concat.append(quarterly_T)

        # 4. Combine and Clean
        if dfs_to_concat:
            combined_df = pd.concat(dfs_to_concat)
            combined_df.index.name = 'Date' # Set index name
            combined_df.reset_index(inplace=True) # Move Date to column
            
            # Ensure Date is datetime
            combined_df['Date'] = pd.to_datetime(combined_df['Date'])
            
            return combined_df
            
        return pd.DataFrame()

    except Exception as e:
        print(f"Error fetching financials for {ticker}: {e}")
        return pd.DataFrame()

# --- Update Functions ---

def update_stock_prices(tickers_df: pd.DataFrame):
    """
    Updates stock prices using batch extraction and vectorized cleaning.
    Enforces a strict 5-year rolling window for the Galaxy Schema.
    """
    prices_folder = stocks_folder / 'prices'
    prices_folder.mkdir(parents=True, exist_ok=True)
    prices_log = load_prices_log()
    
    all_new_data = []
    cutoff_date = pd.Timestamp.now().normalize() - pd.DateOffset(years=5)

    # Phase 1: Extraction
    for _, row in tickers_df.iterrows():
        ticker = row['Ticker']
        last_date_str = prices_log.get(ticker)
        start_date = pd.to_datetime(last_date_str) + pd.Timedelta(days=1) if last_date_str else cutoff_date

        if start_date.date() <= pd.Timestamp.now().date():
            new_data = fetch_prices(ticker, start=start_date.strftime('%Y-%m-%d'))
            if not new_data.empty:
                all_new_data.append(new_data)

    if not all_new_data:
        print("Database is already up to date.")
        return

    # Phase 2: Vectorized Cleaning
    combined_new_df = pd.concat(all_new_data)
    cleaned_new_df = clean_prices(combined_new_df)

    # Phase 3: Distribution and Persistence
    for ticker in cleaned_new_df['Ticker'].unique():
        ticker_new_data = cleaned_new_df[cleaned_new_df['Ticker'] == ticker].copy()
        stock_prices_file = prices_folder / f"{ticker}.parquet"
        
        if stock_prices_file.exists():
            existing_data = pd.read_parquet(stock_prices_file)
            # Combine and enforce the 5-year rolling window 
            updated_data = pd.concat([existing_data, ticker_new_data])
            updated_data = updated_data[updated_data['Date'] >= cutoff_date]
            updated_data = updated_data.drop_duplicates(subset=['Date'], keep='last')
        else:
            updated_data = ticker_new_data

        updated_data.to_parquet(stock_prices_file)
        prices_log[ticker] = str(updated_data['Date'].max().date())

    save_prices_log(prices_log)
    print("Batch price update completed successfully.")

def update_stock_metadata(tickers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Updates the dimension table (dim_ticker).
    Strictly follows 7-day rule using the CSV 'lastUpdated' column.
    """
    metadata_list = []

    # Load existing CSV
    if dim_ticker_file.exists():
        try:
            existing_metadata = pd.read_csv(dim_ticker_file)
        except Exception as e:
            print(f"Error reading metadata file: {e}. Starting fresh.")
            existing_metadata = pd.DataFrame(columns=['Ticker', 'lastUpdated'])
    else:
        # Recovery Mode
        print(f"⚠️  {dim_ticker_file.name} is missing. Starting full metadata rebuild for {len(tickers_df)} tickers...")
        existing_metadata = pd.DataFrame(columns=['Ticker', 'lastUpdated'])

    for _, row in tickers_df.iterrows():
        ticker = row['Ticker']
        
        # Check if we have fresh data (7-day rule)
        if ticker in existing_metadata['Ticker'].values:
            last_updated_vals = existing_metadata.loc[existing_metadata['Ticker'] == ticker, 'lastUpdated'].values
            
            # If we have a date, check if it's recent
            if len(last_updated_vals) > 0 and pd.notna(last_updated_vals[0]):
                try:
                    last_updated = pd.to_datetime(last_updated_vals[0])
                    if (pd.Timestamp.now() - last_updated).days < 7:
                        # Data is fresh enough, skip API call
                        continue 
                except Exception:
                    pass # Date parsing failed, fetch new data

        # Fetch new data (Missing file, missing ticker, or old data)
        ticker_metadata = fetch_metadata(ticker)
        
        if ticker_metadata:
            # Ensure timestamp is set here
            ticker_metadata['lastUpdated'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            metadata_list.append(ticker_metadata)

    # Save changes
    if metadata_list:
        new_metadata_df = pd.DataFrame(metadata_list)
        
        if not existing_metadata.empty:
            combined_metadata = pd.concat([existing_metadata, new_metadata_df])
            # Keep the NEWEST version of the duplicate
            combined_metadata = combined_metadata.drop_duplicates(subset=['Ticker'], keep='last')
        else:
            combined_metadata = new_metadata_df
            
        combined_metadata.to_csv(dim_ticker_file, index=False)
        print(f"✅ Dimension table updated with {len(new_metadata_df)} new/updated records.")
        return combined_metadata
    else:
        print("No metadata updates needed (all local data is < 7 days old).")
        return existing_metadata


def update_stock_financials(tickers_df: pd.DataFrame):
    """
    Updates the financials (fundamental) data for all tickers.
    Saves as Parquet files in a 'financials' subfolder.
    """
    financials_folder = stocks_folder / 'financials'
    financials_folder.mkdir(parents=True, exist_ok=True)
    
    for _, row in tickers_df.iterrows():
        ticker = row['Ticker']
        financials_file = financials_folder / f"{ticker}.parquet"
        
        # Financials data is overwrited in case it's updated or corrected by yfinance
        new_data = fetch_financials(ticker)
        
        if not new_data.empty:
            # Convert columns to string to avoid Parquet schema issues 
            new_data.columns = new_data.columns.astype(str)
            
            new_data.to_parquet(financials_file)
            print(f"Financials for {ticker} saved to {financials_file}")

# --- Update execution ---

def update_from_dashboard():
    """
    Wrapper function to update stock database from the dashboard.
    """
    

def update_stock_database():    
    """
    Updates both stock prices and metadata databases.
    """
    tickers_df = load_tickers()   
    stocks_folder.mkdir(parents=True, exist_ok=True)
    update_stock_prices(tickers_df)
    update_stock_metadata(tickers_df)
    update_stock_financials(tickers_df)

if __name__ == "__main__":
    """
    Execution to update stock database from command line.
    """
    update_stock_database()    
  