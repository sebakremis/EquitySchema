# EquitySchema: Financial Data Engineering & Analytics

ETL framework for equity markets, extracts historical pricing and metadata via Python

![Power BI Dashboard](images/nvda_analysis_annual.png)

## 📌 Project Overview
**EquitySchema** is a full-stack data analytics project designed to visualize the correlation between equity price action and corporate fundamentals. 

The core challenge addressed is the **"Granularity Mismatch"** in financial data: Stock prices move daily (or millisecondly), while revenue/earnings are reported quarterly or annually. This project builds a robust **Galaxy Schema** in Power BI to bridge these two timelines, allowing for seamless analysis of how fundamental performance drives (or lags) stock valuation.

## 🏗 Architecture & Tech Stack

* **ETL Pipeline:** Python (`yfinance`, `pandas`)
* **Data Modeling:** Power BI (Galaxy Schema)
* **Analytics:** DAX (Time Intelligence, Dynamic Granularity)
* **Visualization:** Power BI (Drill-down hierarchies, KPI Cards)

### The Data Pipeline
1.  **Extraction:** Python script fetches 5 years of OHLCV (Price) data and Fundamental (Income Statement) data via the `yfinance` API.
2.  **Transformation:**
    * Enforces a strict **5-year rolling window**.
    * Normalizes "Wide" financial data into "Long/Tidy" format for analysis.
    * **Data Stewardship:** Implements a backfill strategy to handle missing API historical data (e.g., manually appending 2020-2021 SEC filing data for NVDA).
3.  **Loading:** Exports processed data to CSV/Parquet for Power BI ingestion.

## 🧠 Data Model: The Galaxy Schema
Instead of a simple flat file, this project uses a professional **Galaxy Schema** (Multiple Fact Tables) to ensure accurate filtering and performance.

* **Fact Tables:**
    * `fact_Prices`: Daily granularity (Open, Close, Volume).
    * `fact_Financials`: Quarterly/Annual granularity (Revenue, Net Income).
* **Dimension Tables:**
    * `dim_Date`: Continuous date table (Daily) linked to both fact tables.
    * `dim_Ticker`: Metadata (Company Name, Sector).

## 🚀 Key Features & DAX Implementation

### 1. Dynamic Drill-Down (Year vs. Quarter)
Users can view a 5-year trend and "drill down" into a specific year to see quarterly performance. A custom DAX measure detects the scope and switches the calculation logic automatically.

```dax
Revenue (Dynamic) = 
VAR CurrentScope = ISINSCOPE(dim_Date[Year-Qtr]) 
RETURN
    IF(
        CurrentScope, 
        [Total Revenue (Quarterly)], 
        [Total Revenue (Annual)]
    )
```
### 2. Time Intelligence KPIs
Top-level cards show "Headline Metrics" with conditional formatting to indicate positive/negative trends.

* Daily Change %: Calculates momentum vs. the previous trading day.

* YTD Return: Tracks performance since the start of the fiscal year.

## 📂 Project Structure

This repository is organized to separate the **ETL Logic** (Python) from the **Analytics Layer** (Power BI). To maintain a lightweight repository, the binary `.pbix` file (containing cached data) is not tracked. Instead, a **Power BI Template (.pbit)** is provided.

```text
├── EquitySchema_Template.pbit   # 📊 ENTRY POINT: Power BI Template (Schema & Measures only)
├── src/                         # 🐍 Python ETL scripts
│   └── etl.py                   # Main script to fetch & update data
├── data/                        # 💾 Flat file storage (Populated by Python)
│   ├── all_tickers.csv          # Input list of tickers to track
│   ├── etfs.csv                 # Input list of ETFs
│   └── stocks/
│       ├── dim_ticker.csv       # Dimension Table: Company metadata
│       ├── update_log.txt       # Audit log of the last run
│       ├── financials/          # Fact Table folder: Income Statement data
│       └── prices/              # Fact Table folder: Daily OHLCV data
└── images/                      # 📸 Screenshots for documentation
```

## ⚙️ How to Run Locally
1. Clone the Repo

  ```bash  
  git clone https://github.com/sebakremis/EquitySchema.git
  cd EquitySchema
  ```
  
2. Run the Python ETL

  ```bash  
  pip install pandas yfinance
  python src/etl.py
  ```
  
3. Open the Dashboard in Power BI Desktop

Use EquitySchema_Template.pbit and refresh data.

## 📌 Disclaimer
This project was developed strictly for **academic and educational purposes**. 
It does **not** provide financial advice, investment recommendations, or trading guidance.

## 📜 License
This repository is publicly available under the terms of the **MIT License**. You are free to use, modify, and distribute the code for any purpose.

For full details, please see the [MIT License](LICENSE) file included in this repository.
