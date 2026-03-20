import pandas as pd
import glob
import os
import logging
from sqlalchemy import create_engine, text
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
# For PostgreSQL: "postgresql://username:password@localhost:5432/market_db"
# For BigQuery: "bigquery://project-id/dataset_id"
# We will default to a local SQLite database for demonstration purposes.
DEFAULT_CSV_DIRECTORY = "./Sample"
DEFAULT_DB_URL = "sqlite:///./Sample/market_data.db"

RAW_TABLE_NAME = "raw_market_data"
UNIFIED_TABLE_NAME = "unified_time_series"

def parse_args():
    parser = argparse.ArgumentParser(description="Ingestion Engine for Market Data CSVs")
    parser.add_argument('--csv-dir', type=str, default=DEFAULT_CSV_DIRECTORY, help='Directory containing CSV files')
    parser.add_argument('--db-url', type=str, default=DEFAULT_DB_URL, help='Database connection URL (PostgreSQL, SQLite, BigQuery)')
    return parser.parse_args()

def ingest_and_clean_data(csv_dir):
    """
    Reads CSVs from the directory, performs initial cleaning, and aligns formatting.
    """
    logging.info(f"[*] Reading CSVs from {csv_dir}...")
    
    # Allows reading multiple CSV files if needed
    all_files = glob.glob(os.path.join(csv_dir, "*.csv"))
    
    if not all_files:
        logging.warning("[-] No CSV files found.")
        return None
        
    df_list = []
    for file in all_files:
        logging.info(f"   -> Reading {os.path.basename(file)}...")
        try:
            df = pd.read_csv(file)
            df_list.append(df)
        except Exception as e:
            logging.error(f"   -> Failed to read {os.path.basename(file)}: {e}")
        
    if not df_list:
        logging.warning("[-] No valid CSV data could be read.")
        return None

    # Combine datasets programmatically 
    combined_df = pd.concat(df_list, ignore_index=True)
    initial_rows = len(combined_df)
    
    logging.info("[*] Cleaning data...")
    # 1. Handle missing dates by removing rows with no dates
    combined_df = combined_df.dropna(subset=['Date'])
    
    # 2. Consistent date formatting (standardize to YYYY-MM-DD datetime)
    combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
    combined_df = combined_df.dropna(subset=['Date']) # Drop rows where date parsing completely failed
    
    # 3. Handle inconsistent formatting for categorical columns
    if 'Market_Type' in combined_df.columns:
        combined_df['Market_Type'] = combined_df['Market_Type'].str.upper().str.strip()
    
    if 'Market_Name' in combined_df.columns:
        combined_df['Market_Name'] = combined_df['Market_Name'].str.title().str.strip()
        
    # 4. Fill missing numeric values
    numeric_cols = combined_df.select_dtypes(include=['number']).columns
    combined_df[numeric_cols] = combined_df[numeric_cols].fillna(0) # or replace with mean, median, etc. depending on business logic
    
    final_rows = len(combined_df)
    logging.info(f"[+] Cleaned data. Retained {final_rows} out of {initial_rows} rows.")
    return combined_df

def load_to_database(df, engine):
    """
    Loads the cleaned dataframe into the database.
    """
    logging.info(f"[*] Loading data into `{RAW_TABLE_NAME}` table in the database...")
    # We use 'replace' to overwrite the table if it exists. For incremental loading, use 'append'.
    df.to_sql(RAW_TABLE_NAME, engine, if_exists='replace', index=False, chunksize=1000)
    logging.info("[+] Data loaded successfully.")

def transform_data(engine):
    """
    Executes a SQL query to create a unified time-series table where indicators are aligned by date.
    """
    logging.info(f"[*] Transforming data to create `{UNIFIED_TABLE_NAME}`...")
    
    # This SQL query achieves the transformation:
    # We group by Date to create a time-series.
    # While macro-indicators (Inflation, Interest, Currency Strength) are broadly global and identical on a specific date,
    # we take their MAX/AVG. Asset-specific metrics are averaged or summed across instruments.
    # If using BigQuery or PostgreSQL, the SQL dialect remains mostly standard SQL.
    
    # Note: Using IF EXISTS and DROP before CREATE ensures idempotency.
    drop_sql = f"DROP TABLE IF EXISTS {UNIFIED_TABLE_NAME};"
    
    transform_sql = f"""
    CREATE TABLE {UNIFIED_TABLE_NAME} AS
    SELECT 
        Date,
        AVG(Opening_Price) AS Avg_Opening_Price,
        AVG(Closing_Price) AS Avg_Closing_Price,
        SUM(Trading_Volume) AS Total_Trading_Volume,
        SUM(Market_Cap) AS Total_Market_Cap,
        AVG(Volatility_Index) AS Global_Volatility_Index,
        AVG(News_Sentiment) AS Global_News_Sentiment,
        AVG(Social_Sentiment) AS Global_Social_Sentiment,
        AVG(Economic_Impact_Score) AS Economic_Impact_Score,
        MAX(Inflation_Rate) AS Inflation_Rate,
        MAX(Interest_Rate) AS Interest_Rate,
        MAX(Currency_Strength_Index) AS Currency_Strength_Index
    FROM {RAW_TABLE_NAME}
    GROUP BY Date
    ORDER BY Date;
    """
    
    # Executes the statements
    with engine.begin() as conn:
        conn.execute(text(drop_sql))
        conn.execute(text(transform_sql))
        
    logging.info(f"[+] Transformation complete. Unified time-series table `{UNIFIED_TABLE_NAME}` created.")

def main():
    args = parse_args()
    
    # Create database engine
    try:
        engine = create_engine(args.db_url)
    except Exception as e:
        logging.error(f"[-] Failed to connect to database: {e}")
        return
    
    # Run the ingestion and transformation pipeline
    cleaned_data = ingest_and_clean_data(args.csv_dir)
    
    if cleaned_data is not None:
        try:
            load_to_database(cleaned_data, engine)
            transform_data(engine)
            logging.info("[✓] Ingestion Engine pipeline finished successfully.")
        except Exception as e:
            logging.error(f"[-] Pipeline failed during DB operations: {e}")

if __name__ == "__main__":
    main()
