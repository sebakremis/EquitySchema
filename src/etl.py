# src/etl.py
import streamlit as st
import pandas as pd
import yfinance as yf
from .config import (
    DATA_DIR, stocks_folder, dim_ticker_file
    )
from src.core import load_tickers

# Define the log file path
log_file = stocks_folder/'update_log.txt'  
   
def fetch_prices(ticker: str, period: str = None, start: str = None, interval: str = '1d') -> pd.DataFrame:
    """
    Fetch historical data for a given ticker using yfinance.
    Now includes 'Ticker' column for Star Schema linkage.
    """
    data = pd.DataFrame()
    try:
        yf_ticker = yf.Ticker(ticker)
        if period:
            data = yf_ticker.history(period=period, interval=interval)
        elif start:
            data = yf_ticker.history(start=start, interval=interval)
        else:
            raise ValueError("Either 'period' or 'start' must be provided.")
        
        if data.empty:
            return pd.DataFrame()

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
        
        # Add ticker column for using it as a Foreign Key
        data['Ticker'] = ticker
        
        # Reset index so 'Date' becomes a column
        data.reset_index(inplace=True)

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
    Updates stock prices, ensuring only the last 5 years of data are kept.
    Fixes Date column handling and enforces a rolling 5-year window.
    """
    prices_folder = stocks_folder / 'prices'
    prices_folder.mkdir(parents=True, exist_ok=True)

    # Define the 5-year rolling window cutoff
    cutoff_date = pd.Timestamp.now().normalize() - pd.DateOffset(years=5)
    
    # Track if any updates were made
    last_update = None
    
    for _, row in tickers_df.iterrows():
        ticker = row['Ticker']
        stock_prices_file = prices_folder/f"{ticker}.parquet"
        
        updated_data = pd.DataFrame()

        if stock_prices_file.exists():
            # 1. Load existing data
            existing_data = pd.read_parquet(stock_prices_file)
            
            # Ensure 'Date' is datetime and set as index for calculation
            if 'Date' in existing_data.columns:
                existing_data['Date'] = pd.to_datetime(existing_data['Date'])
                existing_data.set_index('Date', inplace=True)
            
            if not existing_data.empty:
                # 2. Prune old data (enforce 5-year limit)
                existing_data = existing_data[existing_data.index >= cutoff_date]
                
                # 3. Determine start date for NEW data
                last_date = existing_data.index.max().date()
                new_start_date = pd.Timestamp(last_date) + pd.Timedelta(days=1)
                
                if new_start_date < pd.Timestamp.today().normalize():
                    new_data = fetch_prices(ticker, start=new_start_date.strftime('%Y-%m-%d'))
                    
                    if not new_data.empty:
                        # fetch_prices returns Date as a column, so set index to match existing_data
                        new_data['Date'] = pd.to_datetime(new_data['Date'])
                        new_data.set_index('Date', inplace=True)
                        
                        updated_data = pd.concat([existing_data, new_data])
                        # Remove duplicates just in case
                        updated_data = updated_data[~updated_data.index.duplicated(keep='last')]
                    else:
                        updated_data = existing_data
                else:
                    updated_data = existing_data
            else:
                # Existing file was empty/corrupt, re-fetch all
                updated_data = fetch_prices(ticker, period='5y')
                if not updated_data.empty:
                    updated_data['Date'] = pd.to_datetime(updated_data['Date'])
                    updated_data.set_index('Date', inplace=True)

        else:
            # File doesn't exist, fetch fresh 5y data
            updated_data = fetch_prices(ticker, period='5y')
            if not updated_data.empty:
                updated_data['Date'] = pd.to_datetime(updated_data['Date'])
                updated_data.set_index('Date', inplace=True)

        # Save Logic
        if not updated_data.empty:
            # Ensure we are saving Date as a column (standard for Power BI Parquet)
            updated_data.reset_index(inplace=True)
            updated_data.to_parquet(stock_prices_file)
            print(f"Updated data for {ticker} saved to {stock_prices_file} (5y window enforced)")
            last_update = pd.Timestamp.now().date()

    if last_update:
        log_updates()

def update_stock_metadata(tickers_df: pd.DataFrame):
    """
    Updates the dimension table (dim_ticker) with the latest information.
    Renamed from 'metadata' to 'dim_ticker' to enforce Star Schema naming.
    """
    metadata_list = []

    # Check if the dimension file exists
    if dim_ticker_file.exists():
        existing_metadata = pd.read_csv(dim_ticker_file)
    else:
        existing_metadata = pd.DataFrame(columns=['Ticker', 'lastUpdated'])

    for _, row in tickers_df.iterrows():
        ticker = row['Ticker']
       
        # Check if ticker is already present and updated recently
        if ticker in existing_metadata['Ticker'].values:
            # Safely get the last update time
            last_updated_str = existing_metadata.loc[existing_metadata['Ticker'] == ticker, 'lastUpdated'].values[0]
            
            if last_updated_str:
                try:
                    last_updated = pd.to_datetime(last_updated_str)
                    # If updated less than 7 days ago, skip
                    if (pd.Timestamp.now() - last_updated).days < 7:
                        print(f"Dimension data for {ticker} is up to date.")
                        continue
                except Exception:
                    # If date parsing fails, force update
                    pass

        # Fetch new metadata if needed
        ticker_metadata = fetch_metadata(ticker)
        if ticker_metadata: # Only append if we got data back
            metadata_list.append(ticker_metadata)

    # Save to CSV
    if metadata_list:
        new_metadata_df = pd.DataFrame(metadata_list)
        
        if not existing_metadata.empty:
            # Combine old and new, keeping the latest version of duplicates
            combined_metadata = pd.concat([existing_metadata, new_metadata_df])
            combined_metadata = combined_metadata.drop_duplicates(subset=['Ticker'], keep='last')
        else:
            combined_metadata = new_metadata_df
            
        combined_metadata.to_csv(dim_ticker_file, index=False)
        print(f"Dimension table updated and saved to {dim_ticker_file}")
    else:
        print("No new metadata to update.")


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
    try:
        with open(log_file, 'r') as f:
            last_update = f.readlines()[-1].strip() # get last line
            st.write(f"Last update:- {last_update}")
    except FileNotFoundError:
        st.write("- No updates logged yet.")
    if st.button("Update All Tickers Data"):
        with st.spinner("Updating data... This may take a while."):           
            update_stock_database()
        print("Stock database updated successfully from dashboard.")
        st.rerun()

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
  