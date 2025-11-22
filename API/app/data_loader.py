import pandas as pd
import numpy as np
from pydantic import ValidationError
from .models import FootballResult # Relative import of your Pydantic model
import os

CSV_FILE = 'football_data_combined.csv' # File created in Step 1

def load_data():
    """Reads the CSV, cleans it, and validates it."""
    if not os.path.exists(CSV_FILE):
        print(f"Error: CSV file '{CSV_FILE}' not found. Please run the consolidation script first.")
        return []
        
    try:
        df = pd.read_csv(CSV_FILE)
        
        # Data Cleaning/Coercion (handling NaNs for optional fields)
        df['result_text'] = df['result_text'].replace({np.nan: None})
        df['bet_value'] = df['bet_value'].replace({np.nan: None})
        df['home_score'] = df['home_score'].astype(int)
        df['away_score'] = df['away_score'].astype(int)
        
        data_list = df.to_dict(orient='records')
        
        # Validate data against Pydantic Model
        validated_data = []
        for item in data_list:
            try:
                validated_data.append(FootballResult(**item))
            except ValidationError as e:
                # Log or handle rows that fail validation
                print(f"Skipping invalid row: {item}. Error: {e}")
                
        return validated_data
        
    except Exception as e:
        print(f"A critical error occurred while loading the file: {e}")
        return []

# The data is loaded once when the application starts
results_data = load_data()