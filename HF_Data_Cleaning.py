'''
# Forensic Financial Audit and Portfolio Performance Reconstruction
# INPUT: Excel file <HF_Trade_History.xlsx> containing raw transaction logs
# OUTPUT: CSV file <HF_Audit_Summary.csv> with split-adjusted performance metrics

# This script automates the recovery and analysis of a 26-year trading history by:
# 1. Cleaning raw ledger data and resolving missing tickers using historical name maps.
# 2. Adjusting historical share counts/prices for stock splits via yfinance API.
# 3. Calculating key performance indicators: VWAP, Total P/L, and CAGR.
# 4. Identifying position lifecycles, including closed positions and re-entries.
# 5. Exporting a consolidated summary for portfolio auditing and reporting.
'''
# Comments can be read as follows
# 1. HEADER PURPOSE: First line describes what the code block does.
# 2. HEADER INPUT: Second line lists names and data types inside <brackets>.
# 3. HEADER OUTPUT: Third line lists resulting names and data types inside <brackets>.
# 4. INLINE COMMENTS: Use the "#<" marker for logic explanations on the same line.
# Happy Coding Patty and Shy

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
import sys

# Define file paths and global ticker correction maps
# INPUT: None
# OUTPUT: Strings <INPUT_FILE>, <OUTPUT_FILE>, Dicts <TICKER_FIXES>, <NAME_TO_TICKER_MAP>
INPUT_FILE = 'HF_Trade_History.xlsx'
OUTPUT_FILE = 'HF_Audit_Summary.csv'

TICKER_FIXES = {
    'FB': 'META',
    'ANTM': 'ELV',
}


# Need to find a better way to do this
# Dennis mentioned WRDS, CRSP, and Compustat
NAME_TO_TICKER_MAP = {
    'EXXON MOBIL': 'XOM',
    'S&P DEP. RECEIPTS': 'SPY',
    'VANGUARD INDEX 500': 'VOO',
    'FRONTIER AIRLINES': 'ULCC',
    'ISPAT INTERNATIONAL': 'MT',
    'WORLDCOM': 'WCOMQ',
    'VISTRA CORP COM': 'VST',
    'TRANSMEDICS GROUP INC': 'TMDX',
    'SOFI TECHNOLOGIES INC': 'SOFI'
}

def generate_audit_csv(input_path, output_path):
    print("Initializing Forensic Audit: Reconstructing 26-Year History...")
    
    try:
        # Load raw Excel data and normalize column headers
        # INPUT: String <input_path>
        # OUTPUT: Pandas DataFrame <df>
        df = pd.read_excel(input_path) #< Read the source trade history file
        df.columns = [str(c).strip() for c in df.columns] #< Remove leading/trailing whitespace from column names
        
        # Resolve missing tickers and apply historical name changes
        # INPUT: Pandas DataFrame <df>, Dict <NAME_TO_TICKER_MAP>, Dict <TICKER_FIXES>
        # OUTPUT: Pandas DataFrame <df> with cleaned 'Ticker' column
        df['Stock'] = df['Stock'].astype(str).str.strip().str.upper() #< Normalize stock names to uppercase
        df['Ticker'] = df['Ticker'].astype(str).str.strip().str.upper() #< Normalize ticker symbols to uppercase
        df['Ticker'] = df['Ticker'].replace(['NAN', 'NONE', '', 'NAT'], np.nan) #< Convert empty or invalid strings to true NaN values
        df['Ticker'] = df['Ticker'].fillna(df['Stock'].map(NAME_TO_TICKER_MAP)) #< Fill missing tickers using the company name map
        df['Ticker'] = df['Ticker'].replace(TICKER_FIXES) #< Update delisted or changed tickers to current symbols
        
        # Clean data types and remove incomplete records
        # INPUT: Pandas DataFrame <df>
        # OUTPUT: Pandas DataFrame <df> sorted chronologically
        df['Trade'] = df['Trade'].astype(str).str.strip().str.upper() #< Standardize trade action labels
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce') #< Convert date column to datetime objects
        for col in ['Shares', 'Price', 'Value']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0) #< Force transaction values to numeric floats
        df = df.dropna(subset=['Date', 'Ticker']).sort_values('Date') #< Remove rows missing dates/tickers and sort by time

        audit_results = []
        today = datetime.now()
        
        # Process each ticker to adjust for splits and calculate performance
        # INPUT: Pandas DataFrame <df>
        # OUTPUT: List <audit_results> containing calculated metrics
        for ticker, group in df.groupby('Ticker'):
            if ticker in ['NAN', 'NONE', '']: continue
            
            # Reconstruct share counts based on historical split data
            # INPUT: Ticker <ticker>, Pandas DataFrame <group>
            # OUTPUT: Pandas DataFrame <group> with split-adjusted 'Shares' and 'Price'
            current_price = None
            try:
                stock_obj = yf.Ticker(ticker) #< Initialize yfinance API connection for the ticker
                splits = stock_obj.splits #< Retrieve historical stock split events
                if splits is not None and not splits.empty:
                    for split_date, ratio in splits.items():
                        mask = group['Date'] < split_date.tz_localize(None) #< Identify trades occurring before a split
                        group.loc[mask, 'Shares'] *= ratio #< Adjust historical share count by split ratio
                        group.loc[mask, 'Price'] /= ratio #< Adjust historical price by split ratio
                
                live_price = stock_obj.fast_info.get('lastPrice') #< Fetch the most recent market price
                if live_price and not np.isnan(live_price):
                    current_price = float(live_price) #< Set current price to live market data
            except:
                pass #< Proceed to fallback logic if API fails
            
            if current_price is None or current_price == 0:
                current_price = float(group['Price'].iloc[-1]) #< Fallback to the last recorded price in the ledger

            # Calculate P/L, average cost, and activity status
            # INPUT: Pandas DataFrame <group>, Float <current_price>
            # OUTPUT: Dictionary of metrics added to <audit_results>
            net_shares = group['Shares'].sum() #< Calculate current net share balance
            market_value = net_shares * current_price #< Calculate current market value of holding
            total_pl = market_value + group['Value'].sum() #< Calculate total gain/loss including realized cash
            buys = group[group['Trade'] == 'BUY'] #< Isolate purchase transactions
            total_bought_shares = buys['Shares'].sum() #< Sum all shares ever purchased
            invested_cap = abs(buys['Value'].sum()) #< Calculate total capital deployed for buys
            avg_buy_price = (invested_cap / total_bought_shares) if total_bought_shares > 0 else 0 #< Calculate VWAP for the position
            
            is_exited = "Yes" if net_shares <= 0.001 else "No" #< Determine if the position is currently closed
            group['Running_Balance'] = group['Shares'].cumsum() #< Track the share balance over time
            was_ever_zero = (group['Running_Balance'].apply(lambda x: abs(x) < 0.001)).any() #< Check if balance ever hit zero
            re_entered = "Yes" if (was_ever_zero and net_shares > 0.001) else "No" #< Check if position was restarted
            
            first_date = group['Date'].min() #< Locate the earliest trade date
            years_held = (today - first_date).days / 365.25 #< Calculate holding duration in years
            bi_annual_periods = round(years_held * 2, 2) #< Convert years to bi-annual count
            
            cagr = 0.0
            if invested_cap > 0:
                final_val = invested_cap + total_pl #< Calculate final capital for CAGR formula
                if final_val > 0:
                    cagr = (pow(final_val / invested_cap, 1/max(years_held, 0.01)) - 1) * 100 #< Calculate annual growth percentage

            audit_results.append({
                'Ticker': ticker,
                'Initial_Year': first_date.year,
                'Avg_Price_Paid': round(avg_buy_price, 2),
                'Current_Price': round(current_price, 2),
                'Current_Shares': round(net_shares, 2),
                'Position_Size_USD': round(market_value, 2),
                'Total_PL_USD': round(total_pl, 2),
                'CAGR_Pct': round(cagr, 2),
                'Exited': is_exited,
                'Re_Entered': re_entered,
                'Bi_Annual_Periods': bi_annual_periods,
                'Years_Held': round(years_held, 2)
            })

        # Finalize and export results to CSV
        # INPUT: List <audit_results>, String <output_path>
        # OUTPUT: CSV file at <output_path>
        result_df = pd.DataFrame(audit_results) #< Convert results list to a structured DataFrame
        result_df.to_csv(output_path, index=False) #< Save the audit summary to a CSV file
        return True

    except Exception as e:
        raise e

if __name__ == "__main__":
    try:
        generate_audit_csv(INPUT_FILE, OUTPUT_FILE)
        print(f"STATUS: SUCCESS")
        print(f"PATH: {OUTPUT_FILE}")
    except PermissionError:
        print(f"STATUS: FAILED")
        print(f"ERROR: Permission denied. Please CLOSE '{OUTPUT_FILE}' in Excel and try again.")
    except Exception as e:
        print(f"STATUS: FAILED")

        print(f"ERROR: {str(e)}")

