# src/etl.py
import pandas as pd
import yfinance as yf
from pathlib import Path
from .config import DATA_DIR, stocks_folder, all_tickers_file, metadata_file

# Define the log file path
log_file = stocks_folder/'update_log.txt'

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
    
   
def fetch_prices(ticker: str, period: str = None, start: str = None, interval: str = '1d') -> pd.DataFrame:
    """
    Fetch historical data for a given ticker using yfinance.
    It accepts either a period or a start date.
    """
    data = pd.DataFrame()
    try:
        yf_ticker = yf.Ticker(ticker) # Initialize yfinance Ticker object
        if period:
            data = yf_ticker.history(period=period, interval=interval)
        elif start:
            data = yf_ticker.history(start=start, interval=interval)
        else:
            raise ValueError("Either 'period' or 'start' must be provided.")
        
        # Format column names
        data.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
            'Dividends': 'dividends',
            'Stock Splits': 'stockSplits'
        }, inplace=True)

        return data
    except Exception as e:
        print(f"Error fetching data for ticker {ticker}: {e}")
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
            # last updated timestamp
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


def log_updates():
    """
    Logs the last time the stock prices were updated.
    Creates both the log file and the parent folder if they do not exist.
    """
    
    stocks_folder.mkdir(parents=True, exist_ok=True)

    with open(log_file, 'a') as f:
        f.write(pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
    print(f"Update logged in {log_file}")

def update_stock_prices(tickers_df: pd.DataFrame):
    """
    Updates the stock prices database with the latest information for all followed tickers.
    Creates the parket file for the ticker if it does not exist.
    """
    # Track if any updates were made
    last_update = None
    
    for _, row in tickers_df.iterrows():
        ticker = row['Ticker']
        stock_prices_file = stocks_folder/f"prices/{ticker}.parquet"
        
        # --- Fetch price data and update parquet file ---
        if stock_prices_file.exists():
            existing_data = pd.read_parquet(stock_prices_file)
            existing_data.index = pd.to_datetime(existing_data.index)
            if not existing_data.empty:
                # Determine the last date in existing data
                last_date = existing_data.index.max().date()
                new_start_date = pd.Timestamp(last_date) + pd.Timedelta(days=1)
                # Check if new_start_date is in the future
                if new_start_date >= pd.Timestamp.today().normalize():
                    print(f"No new data for {ticker}.")
                    continue            
                new_data = fetch_prices(ticker, start=new_start_date.strftime('%Y-%m-%d'))
                if not new_data.empty:
                    updated_data = pd.concat([existing_data, new_data])
                    updated_data = updated_data[~updated_data.index.duplicated(keep='last')]
                    updated_data.to_parquet(stock_prices_file)
                    print(f"Updated data for {ticker} saved to {stock_prices_file}")
                    last_update = pd.Timestamp.now().date()
            else:
                # If existing data is empty, fetch all available data
                updated_data = fetch_prices(ticker, period='5y')
                updated_data.to_parquet(stock_prices_file)
                print(f"Updated data for {ticker} saved to {stock_prices_file}")
                last_update = pd.Timestamp.now().date()        

        else:
            # If file does not exist, fetch all available data
            updated_data = fetch_prices(ticker, period='5y')
            updated_data.to_parquet(stock_prices_file)
            print(f"Updated data for {ticker} saved to {stock_prices_file}")
            last_update = pd.Timestamp.now().date()

    # Log the update time
    if last_update:
        log_updates()

def update_stock_metadata(tickers_df: pd.DataFrame):
    """
    Updates the stock metadata database with the latest information for all followed tickers.
    """
    metadata_list = []

    for _, row in tickers_df.iterrows():
        ticker = row['Ticker']
       
        # if metadata file exists, check if ticker is already present
        if metadata_file.exists():
            existing_metadata = pd.read_csv(metadata_file)
            if ticker in existing_metadata['Ticker'].values:
                # check last updated date for the ticker
                last_updated_str = existing_metadata.loc[existing_metadata['Ticker'] == ticker, 'lastUpdated'].values[0]               
                if last_updated_str:
                    last_updated = pd.to_datetime(last_updated_str)
                    if (pd.Timestamp.now() - last_updated).days < 7:
                        print(f"Metadata for {ticker} is up to date.")
                        continue
        ticker_metadata = fetch_metadata(ticker)
        metadata_list.append(ticker_metadata)

    # Save metadata to CSV
    if metadata_list:
        metadata_df = pd.DataFrame(metadata_list)
        if metadata_file.exists():
            existing_metadata = pd.read_csv(metadata_file)
            combined_metadata = pd.concat([existing_metadata, metadata_df])
            combined_metadata = combined_metadata.drop_duplicates(subset=['Ticker'], keep='last')
            combined_metadata.to_csv(metadata_file, index=False)
        else:
            metadata_df.to_csv(metadata_file, index=False)
        print(f"Metadata updated and saved to {metadata_file}")


def update_stock_database():    
    """
    Updates both stock prices and metadata databases.
    """
    tickers_df = load_tickers()   
    stocks_folder.mkdir(parents=True, exist_ok=True)
    update_stock_prices(tickers_df)
    update_stock_metadata(tickers_df)

if __name__ == "__main__":
    """
    Execution to update stock database.
    """
    update_stock_database()    
  