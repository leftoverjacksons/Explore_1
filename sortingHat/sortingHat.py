import configparser
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Read config from one level below
config = configparser.ConfigParser()
config.read('../config.ini')

# Create SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{config['mysql']['user']}:{config['mysql']['password']}@{config['mysql']['host']}/{config['mysql']['database']}")

# SQL query to extract relevant data
query = """
SELECT 
    BILL_CUSTOMER_SID, 
    EXTENDED_PRICE
FROM 
    F_SALES
"""

# Execute query and load into DataFrame
df = pd.read_sql(query, engine)

# Calculate total revenue per customer
customer_summary = df.groupby('BILL_CUSTOMER_SID')['EXTENDED_PRICE'].sum().reset_index()
customer_summary.columns = ['CUSTOMER_ID', 'TOTAL_REVENUE']

# Sort customers by revenue
customer_summary = customer_summary.sort_values('TOTAL_REVENUE', ascending=False)

total_revenue = customer_summary['TOTAL_REVENUE'].sum()

# Calculate cumulative revenue percentage
customer_summary['CUM_REVENUE_PERCENT'] = customer_summary['TOTAL_REVENUE'].cumsum() / total_revenue

# Set the target revenue percentage for High-Value customers (easily adjustable)
target_revenue_percent = 0.5  # 50% split, adjust as needed

# Identify high-value customers
high_value_cutoff = customer_summary['CUM_REVENUE_PERCENT'].searchsorted(target_revenue_percent)
customer_summary['SEGMENT'] = np.where(customer_summary.index < high_value_cutoff, 'High-Value', 'Tail')

# Print summary
print("\nCustomer Segmentation Summary:")
print(f"Total customers: {len(customer_summary)}")
print(f"High-value customers: {high_value_cutoff} ({high_value_cutoff/len(customer_summary):.2%})")
print(f"Tail customers: {len(customer_summary) - high_value_cutoff} ({(len(customer_summary) - high_value_cutoff)/len(customer_summary):.2%})")
print(f"High-value revenue %: {customer_summary[customer_summary['SEGMENT'] == 'High-Value']['TOTAL_REVENUE'].sum() / total_revenue:.2%}")
print(f"Tail revenue %: {customer_summary[customer_summary['SEGMENT'] == 'Tail']['TOTAL_REVENUE'].sum() / total_revenue:.2%}")

# Save customer segmentation results
customer_summary.to_csv('customer_segmentation.csv', index=False)

print("\nAnalysis complete. Check the generated CSV file for results.")