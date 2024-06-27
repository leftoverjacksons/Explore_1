import os
import sys
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import configparser

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Go up one level to the parent directory
parent_dir = os.path.dirname(current_dir)

# Construct the path to the config file
config_path = os.path.join(parent_dir, 'config.ini')

# Read database configuration
config = configparser.ConfigParser()
config.read(config_path)

try:
    db_config = config['mysql']
except KeyError:
    print(f"Error: 'mysql' section not found in {config_path}")
    print(f"Available sections: {config.sections()}")
    sys.exit(1)

# Create SQLAlchemy engine
try:
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
except KeyError as e:
    print(f"Error: Missing key in 'mysql' configuration: {e}")
    sys.exit(1)

# Execute SQL queries and load data into pandas DataFrames
try:
    quotes_df = pd.read_sql("SELECT QUOTE_SID, CUSTOMER_SID, DATE_CREATED_SID, EXPIRATION_DATE_SID, QUOTE_VALUE FROM F_SALES_QUOTES", engine)
    sales_df = pd.read_sql("SELECT SALES_DOCUMENT_SID, BILL_CUSTOMER_SID, DOCUMENT_DATE_SID, EXTENDED_PRICE FROM F_SALES", engine)
    customers_df = pd.read_sql("SELECT CUSTOMER_SID, CUSTOMER_CODE FROM D_SALES_DOCUMENTS", engine)
except Exception as e:
    print(f"Error executing SQL queries: {e}")
    sys.exit(1)

# Merge customer data with quotes and sales
quotes_df = quotes_df.merge(customers_df, on='CUSTOMER_SID', how='left')
sales_df = sales_df.merge(customers_df, left_on='BILL_CUSTOMER_SID', right_on='CUSTOMER_SID', how='left')

# Convert date SIDs to datetime
quotes_df['QUOTE_DATE'] = pd.to_datetime(quotes_df['DATE_CREATED_SID'], format='%Y%m%d')
quotes_df['EXPIRATION_DATE'] = pd.to_datetime(quotes_df['EXPIRATION_DATE_SID'], format='%Y%m%d')
sales_df['SALE_DATE'] = pd.to_datetime(sales_df['DOCUMENT_DATE_SID'], format='%Y%m%d')

# Function to match quotes with sales
def match_quote_to_sale(row, sales_df):
    customer_sales = sales_df[sales_df['CUSTOMER_CODE'] == row['CUSTOMER_CODE']]
    matching_sales = customer_sales[
        (customer_sales['SALE_DATE'] >= row['QUOTE_DATE']) &
        (customer_sales['SALE_DATE'] <= row['EXPIRATION_DATE']) &
        (customer_sales['EXTENDED_PRICE'] == row['QUOTE_VALUE'])
    ]
    return len(matching_sales) > 0

# Apply matching function
quotes_df['CONVERTED'] = quotes_df.apply(lambda row: match_quote_to_sale(row, sales_df), axis=1)

# Calculate conversion rate
conversion_rate = quotes_df['CONVERTED'].mean()

print(f"Overall quote conversion rate: {conversion_rate:.2%}")

# Additional analysis code here...
# (Include the analysis code from the previous response)

# Save results to CSV in the current directory
customer_conversion.to_csv(os.path.join(current_dir, 'customer_conversion_rates.csv'), index=False)
value_range_conversion.to_csv(os.path.join(current_dir, 'value_range_conversion_rates.csv'))
time_conversion.to_csv(os.path.join(current_dir, 'time_conversion_rates.csv'), index=False)

print("Analysis complete. Results saved in the 'Quote Efficacy' folder.")