import pandas as pd
import numpy as np
import os

# --- Configuration ---
EXCEL_FILE = 'HIST12526.xlsx' 
OUTPUT_FILE = 'football_data_combined.csv'

column_mapping = {
    'Home team': 'home_team',
    'Away team': 'away_team',
    '+0 HG': 'p_home_0',
    '+1 HG': 'p_home_1',
    '+2 HG': 'p_home_2',
    '+0 AG': 'p_away_0',
    '+1 AG': 'p_away_1',
    '+2 AG': 'p_away_2',
    'A1': 'pred_1',
    'A2': 'pred_2',
    'Result': 'result_text',
    'Bet': 'bet_value'
}
# --- End Configuration ---

def run_consolidation():
    print(f"Attempting to read data directly from the Excel file: {EXCEL_FILE}")
    
    try:
        # Read all sheets into a dictionary of DataFrames using 'openpyxl'
        # sheet_name=None reads all sheets in the file
        all_sheets = pd.read_excel(EXCEL_FILE, sheet_name=None)
        
        # Filter out sheets that contain the word 'Sheet' in their name
        data_sheets = {name: df for name, df in all_sheets.items() if 'Sheet' in name}
        
        if not data_sheets:
            print(f"❌ Error: No data sheets (e.g., 'Sheet1') found in {EXCEL_FILE}.")
            print("Please ensure the sheets containing data are named using the word 'Sheet'.")
            return

        print(f"✅ Found {len(data_sheets)} sheets to combine: {list(data_sheets.keys())}")
        
        # Combine all sheets into a single DataFrame
        combined_df = pd.concat(data_sheets.values(), ignore_index=True)

        # Apply the new, standardized column names
        combined_df.rename(columns=column_mapping, inplace=True)
        
        # Data Cleaning: ensure scores are integers and handle missing values
        combined_df['pred_1'] = combined_df['pred_1'].replace({np.nan: 0}).astype(int)
        combined_df['pred_2'] = combined_df['pred_2'].replace({np.nan: 0}).astype(int)
        
        # Save the combined data as a CSV
        combined_df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Success! Combined data saved to: {OUTPUT_FILE}")
        
    except FileNotFoundError:
        print(f"\n❌ Error: The file '{EXCEL_FILE}' was not found.")
        print("Please ensure the file is in the current directory.")
    except Exception as e:
        print(f"\n❌ A critical error occurred during Excel processing: {e}")

if __name__ == '__main__':
    run_consolidation()