"""
main.py

ETL Control Center
Contains the main function to update the database and manage the tickers list.
"""
import streamlit as st
import pandas as pd
from src.core import (
    load_tickers, save_tickers, add_tickers,
    remove_tickers
)
from src.etl import update_stock_database, load_prices_log
from src.config import dim_ticker_file, prices_log_file, stocks_folder

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
        help="Type or paste tickers separated by spaces or commas. You can also place each ticker on a new line.",
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
    Optimized for performance: 
    1. Uses Lazy Loading for metadata (no blocking API calls).
    2. Vectorized check for Financials file existence.
    """
    # Load metadata (if available)
    cols_to_load = ['Ticker', 'shortName', 'sector']
    if dim_ticker_file.exists():
        try:
            metadata_df = pd.read_csv(dim_ticker_file, usecols = cols_to_load)
        except ValueError as e:
            metadata_df = pd.read_csv(dim_ticker_file) # Fallback if cols mismatch
    else:
        metadata_df = pd.DataFrame(columns=cols_to_load)
    
    # Merge Tickers with Metadata
    display_df = pd.merge(tickers_df, metadata_df, on='Ticker', how='left')

    # Merge with Price Log
    prices_log = load_prices_log()    

    # Create DataFrame from items {'AAPL': 'Date'} -> [('AAPL', 'Date')]
    log_df = pd.DataFrame(list(prices_log.items()), columns=['Ticker', 'lastPriceDate'])
    display_df = pd.merge(display_df, log_df, on='Ticker', how='left')
    
    # Check Financials Data
    display_df["financialsData"] = display_df["Ticker"].apply(
        lambda x: "Yes" if (stocks_folder / 'financials' / f"{x}.parquet").exists() else "Nope"
    )
    # Fill NaN values for better UI
    display_df['shortName'] = display_df['shortName'].fillna("Pending Update...")
    display_df['sector'] = display_df['sector'].fillna("-")
    display_df['lastPriceDate'] = display_df['lastPriceDate'].fillna("Pending Update...")

    # Ensure specific column order
    final_cols = ['Ticker', 'shortName', 'sector', 'lastPriceDate', 'financialsData']
    
    # Add missing columns if they don't exist (safety check)
    for col in final_cols:
        if col not in display_df.columns:
            display_df[col] = None
            
    return display_df[final_cols]

# --- Data Explorer ---

def _render_explorer(tickers_df: pd.DataFrame):
    """
    Render the Raw Data Explorer tab.
    Allows viewing Metadata (CSV) or Prices/Financials (Parquet).
    """
    st.subheader("🔍 Data Explorer")
    
    # 1. Select Dataset Type
    dataset_type = st.radio(
        "Select Dataset:", 
        ["Metadata (Dimension)", "Stock Prices (Fact)", "Financials (Fact)"],
        horizontal=True
    )

    # 2. Logic for Metadata (Single File)
    if dataset_type == "Metadata (Dimension)":
        if dim_ticker_file.exists():
            df = pd.read_csv(dim_ticker_file)
            st.markdown(f"**Records:** {len(df)}")
            st.dataframe(df, width='stretch')
        else:
            st.warning("⚠️ Metadata file (dim_ticker.csv) not found.")

    # 3. Logic for Fact Tables (One file per Ticker)
    else:
        # Select Ticker
        selected_ticker = st.selectbox(
            "Select Ticker:", 
            tickers_df['Ticker'].sort_values(), 
            width = 100
            )
        
        if selected_ticker:
            # Determine Path based on selection
            if "Prices" in dataset_type:
                target_file = stocks_folder / 'prices' / f"{selected_ticker}.parquet"
            else:
                target_file = stocks_folder / 'financials' / f"{selected_ticker}.parquet"

            # Load and Display
            if target_file.exists():
                try:
                    df = pd.read_parquet(target_file)
                    
                    # metrics
                    col1, col2, col3 = st.columns(3)
                    with col1: st.metric("Rows", len(df))
                    with col2: st.metric("Columns", len(df.columns))
                    if "Prices" in dataset_type:
                        # Show date range for prices
                        min_date = df.index.min().date() if isinstance(df.index, pd.DatetimeIndex) else df['Date'].min().date()
                        max_date = df.index.max().date() if isinstance(df.index, pd.DatetimeIndex) else df['Date'].max().date()
                        with col3: st.write(f"**Range:** {min_date} to {max_date}")

                    st.dataframe(df, width='stretch')
                except Exception as e:
                    st.error(f"Error reading file: {e}")
            else:
                st.info(f"ℹ️ No {dataset_type} data found for **{selected_ticker}**.")


# --- Main function ---

def main():
    st.title("🎛️ EquitySchema: ETL Control Center")
    st.write("Manage the ticker universe and update the data pipeline for the Power BI Galaxy Schema.")

    # Load tickers for both tabs
    tickers_df = load_tickers()

    # Create tabs
    tab_main, tab_explore = st.tabs(["⚙️ Main Panel", "💾 Data Explorer"])

    # --- Tab 1: Main ---
    with tab_main:
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
        # Count of tickers
        st.markdown(f"**Count of tickers:** {len(tickers_df)}")  

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

    # --- Tab 2: Data Explorer ---
    with tab_explore:
        _render_explorer(tickers_df)

if __name__ == "__main__":
    main()
