"""
main.py

Landing page module.
Contains the main function to update the database and manage the tickers list.
"""
import streamlit as st
import pandas as pd
import json
from src.core import (
    load_tickers, save_tickers, add_tickers,
    remove_tickers
)
from src.etl import update_stock_database, update_stock_metadata
from src.config import dim_ticker_file, prices_log_file

# --- Hide message "Press Ctrl+Enter in st.text_area()" ---
st.markdown("""
    <style>
    /* Hide the specific element that shows the input instructions */
    div[data-testid="InputInstructions"] {
        display: none;
    }
    </style>
    """, unsafe_allow_html=True)

# Page configuration
st.set_page_config(layout="wide", page_title="EquitySchema", page_icon="🎛️")

# --- Tickers management UI ---

@st.dialog(title="Add Tickers")
def _add_tickers_dialog(tickers_df: pd.DataFrame):
    """
    Opens a dialog to add tickers.
    """
    user_input = st.text_area(
        "Tickers to add:",
        help="Type or pase tickers separated by spaces or commas. You can also place each ticker on a new line.",
        height=200
        )
    if st.button("Submit"):
        try:
            updated_tickers_df = add_tickers(user_input, tickers_df)
            save_tickers(updated_tickers_df)
            print("Tickers added successfully.")
        except Exception as e:
            print(f"Error adding tickers: {e}")
        st.rerun()

@st.dialog(title="Remove Tickers")
def _remove_tickers_dialog(tickers_df: pd.DataFrame, tickers_to_remove: list):
    """
    Opens a dialog to remove tickers.
    """
    st.write(f"Removing tickers: **{', '.join(tickers_to_remove)}**")
    if st.button("Confirm"):
        try:
            updated_tickers_df = remove_tickers(tickers_df, tickers_to_remove)
            save_tickers(updated_tickers_df)
            print("Tickers removed successfully.")
        except Exception as e:
            print(f"Error removing tickers: {e}")
        st.rerun()

# --- Dashboard data preparation ---

def _fetch_dashboad_data(tickers_df: pd.DataFrame):
    """
    Fetches and prepares data for display in the dashboard.
    Merges tickers with metadata and price log info.
    """
    # Get info from metadata file
    cols_to_load = ['Ticker', 'shortName', 'sector']
    if dim_ticker_file.exists():
        try:
            metadata_df = pd.read_csv(dim_ticker_file, usecols = cols_to_load)
        except ValueError as e:
            print(f"Warning: Column mismatch. Loading all columns. {e}")
            metadata_df = pd.read_csv(dim_ticker_file)
    else:
        metadata_df = pd.DataFrame(columns=cols_to_load)
    
    # Find if tickers are missing in metadata
    missing_tickers = tickers_df[~tickers_df['Ticker'].isin(metadata_df['Ticker'])]

    # If missing tickers, fetch metadata for them
    if not missing_tickers.empty:
        with st.spinner(f"Fetching metadata for {len(missing_tickers)} new ticker(s)..."):
            update_stock_metadata(missing_tickers)
            # Reload metadata
            metadata_df = pd.read_csv(dim_ticker_file, usecols = cols_to_load)
    
    # Merge tickers with metadata
    display_df = pd.merge(tickers_df, metadata_df, on='Ticker', how='left')

    # Merge with json log file to add the Last Price Update info
    if prices_log_file.exists():
        with open(prices_log_file, 'r') as f:
            prices_log = json.load(f)
        # Create DataFrame from items directly
        # This converts {'AAPL': 'Date'} -> [('AAPL', 'Date')]
        log_df = pd.DataFrame(list(prices_log.items()), columns=['Ticker', 'lastPriceDate'])
        display_df = pd.merge(display_df, log_df, on='Ticker', how='left')
    
    # Define columns order to display in dashboard
    display_df = display_df[['Ticker', 'shortName', 'sector', 'lastPriceDate']]
    return display_df

# --- Main function ---

def main():
    st.title("🎛️ EquitySchema: ETL Control Center")
    st.write("Manage the ticker universe and update the data pipeline for the Power BI Galaxy Schema.")

    # Load tickers and fetch data for display
    tickers_df = load_tickers()
    display_df = _fetch_dashboad_data(tickers_df)        

    # Display table
    st.subheader("Tickers in Database:")
    event = st.dataframe(
        display_df,
        hide_index=True,
        width = 850,
        on_select= "rerun",
        selection_mode="multi-row" 
        )
    
    # Get list of selected tickers from selected rows.
    selected_indices = event.selection.rows # returns a list of numerical indices
    selected_tickers_df = tickers_df.iloc[selected_indices]
    selected_tickers = selected_tickers_df['Ticker'].tolist()

    # Buttons for adding/removing tickers
    col1, col2 = st.columns([1,5])
    with col1:
        if st.button("Remove Selected Tickers", disabled = not selected_tickers, type = "primary"):
            _remove_tickers_dialog(tickers_df, selected_tickers)
    st.markdown("---")
    with col2:
        if st.button("Add Tickers"):
            _add_tickers_dialog(tickers_df)   

    # Update database section
    st.subheader("Update Database")
    if st.button("Update All Tickers Data"):
        with st.spinner("Updating data... This may take a while."):           
            update_stock_database()
        print("Stock database updated successfully from dashboard.")
        st.rerun()

if __name__ == "__main__":
    main()
