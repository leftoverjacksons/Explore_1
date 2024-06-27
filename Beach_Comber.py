import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import configparser
import datetime

# Read config
config = configparser.ConfigParser()
config.read('config.ini')

# Create SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{config['mysql']['user']}:{config['mysql']['password']}@{config['mysql']['host']}/{config['mysql']['database']}")

# SQL query to extract relevant data
query = """
SELECT 
    f.BILL_CUSTOMER_SID, 
    f.ORDER_DATE_SID, 
    f.ITEM_SID, 
    f.QUANTITY_ORDERED, 
    f.UNIT_PRICE, 
    f.EXTENDED_PRICE,
    d.ITEM_NUMBER,
    d.STANDARD_COST_AMOUNT
FROM 
    F_SALES f
JOIN 
    D_ITEMS d ON f.ITEM_SID = d.ITEM_SID
"""

# Execute query and load into DataFrame
df = pd.read_sql(query, engine)

# Convert ORDER_DATE_SID to datetime
df['ORDER_DATE'] = pd.to_datetime(df['ORDER_DATE_SID'], format='%Y%m%d')

# Calculate revenue and profit
df['REVENUE'] = df['EXTENDED_PRICE']
df['PROFIT'] = df['EXTENDED_PRICE'] - (df['QUANTITY_ORDERED'] * df['STANDARD_COST_AMOUNT'])

print("Data loaded and prepared:")
print(df.head())
print(df.info())

# Calculate total revenue and profit per customer
customer_summary = df.groupby('BILL_CUSTOMER_SID').agg({
    'REVENUE': 'sum',
    'PROFIT': 'sum',
    'ORDER_DATE': 'max'  # Last order date
}).reset_index()

# Sort customers by revenue
customer_summary = customer_summary.sort_values('REVENUE', ascending=False)

# Calculate cumulative revenue percentage
customer_summary['CUM_REVENUE_PERCENT'] = customer_summary['REVENUE'].cumsum() / customer_summary['REVENUE'].sum()

# Identify high-value customers (top 20% by revenue)
high_value_cutoff = customer_summary['CUM_REVENUE_PERCENT'].searchsorted(0.8)
customer_summary['SEGMENT'] = np.where(customer_summary.index < high_value_cutoff, 'High-Value', 'Tail')

print("\nCustomer Segmentation:")
print(customer_summary.head())
print(f"Number of high-value customers: {high_value_cutoff}")
print(f"Number of tail customers: {len(customer_summary) - high_value_cutoff}")

# Filter for tail customers
tail_customers = customer_summary[customer_summary['SEGMENT'] == 'Tail']

# Calculate Recency, Frequency, Monetary (RFM) metrics
current_date = datetime.datetime.now()
tail_customers['RECENCY'] = (current_date - tail_customers['ORDER_DATE']).dt.days
tail_customers['FREQUENCY'] = df[df['BILL_CUSTOMER_SID'].isin(tail_customers['BILL_CUSTOMER_SID'])].groupby('BILL_CUSTOMER_SID')['ORDER_DATE'].nunique()
tail_customers['MONETARY'] = tail_customers['REVENUE']

print("\nTail Customer RFM Analysis:")
print(tail_customers.head())
print(tail_customers.describe())

# Define churn risk thresholds
RECENCY_THRESHOLD = tail_customers['RECENCY'].median()  # Median recency
FREQUENCY_THRESHOLD = tail_customers['FREQUENCY'].median()  # Median frequency
MONETARY_THRESHOLD = tail_customers['MONETARY'].median()  # Median monetary value

# Calculate churn risk factors
tail_customers['RECENCY_RISK'] = tail_customers['RECENCY'] > RECENCY_THRESHOLD
tail_customers['FREQUENCY_RISK'] = tail_customers['FREQUENCY'] < FREQUENCY_THRESHOLD
tail_customers['MONETARY_RISK'] = tail_customers['MONETARY'] < MONETARY_THRESHOLD

# Create a simple churn risk score (0-3, where 3 is highest risk)
tail_customers['CHURN_RISK_SCORE'] = (
    tail_customers['RECENCY_RISK'].astype(int) +
    tail_customers['FREQUENCY_RISK'].astype(int) +
    tail_customers['MONETARY_RISK'].astype(int)
)

# Normalize RFM values for visualization (using min-max scaling)
for col in ['RECENCY', 'FREQUENCY', 'MONETARY']:
    min_val = tail_customers[col].min()
    max_val = tail_customers[col].max()
    tail_customers[f'{col}_NORM'] = (tail_customers[col] - min_val) / (max_val - min_val)

print("\nChurn Risk Analysis:")
print(tail_customers.head())
print(tail_customers['CHURN_RISK_SCORE'].value_counts(normalize=True))

# Visualizations
plt.figure(figsize=(10, 6))
sns.histplot(customer_summary['REVENUE'], kde=True, log_scale=True)
plt.title('Revenue Distribution (Log Scale)')
plt.xlabel('Revenue')
plt.savefig('revenue_distribution.png')
plt.close()

fig, axes = plt.subplots(1, 3, figsize=(15, 5))
sns.histplot(tail_customers['RECENCY'], kde=True, ax=axes[0])
axes[0].set_title('Recency Distribution')
sns.histplot(tail_customers['FREQUENCY'], kde=True, ax=axes[1])
axes[1].set_title('Frequency Distribution')
sns.histplot(tail_customers['MONETARY'], kde=True, log_scale=True, ax=axes[2])
axes[2].set_title('Monetary Distribution (Log Scale)')
plt.tight_layout()
plt.savefig('rfm_distributions.png')
plt.close()

plt.figure(figsize=(10, 6))
scatter = plt.scatter(tail_customers['RECENCY_NORM'],
                      tail_customers['FREQUENCY_NORM'],
                      c=tail_customers['CHURN_RISK_SCORE'],
                      s=tail_customers['MONETARY_NORM']*100,
                      cmap='YlOrRd',
                      alpha=0.6)
plt.colorbar(scatter)
plt.title('Tail Customer Churn Risk')
plt.xlabel('Recency (Normalized)')
plt.ylabel('Frequency (Normalized)')
plt.savefig('churn_risk_scatter.png')
plt.close()

# Generate summary report
report = f"""
Customer Churn Analysis Report

1. Customer Segmentation:
   - Total customers: {len(customer_summary)}
   - High-value customers: {high_value_cutoff} ({high_value_cutoff/len(customer_summary):.2%})
   - Tail customers: {len(tail_customers)} ({len(tail_customers)/len(customer_summary):.2%})

2. Tail Customer Analysis:
   - Average Recency: {tail_customers['RECENCY'].mean():.2f} days
   - Average Frequency: {tail_customers['FREQUENCY'].mean():.2f} orders
   - Average Monetary Value: ${tail_customers['MONETARY'].mean():.2f}

3. Churn Risk:
   - Low Risk (0): {(tail_customers['CHURN_RISK_SCORE'] == 0).sum()} customers ({(tail_customers['CHURN_RISK_SCORE'] == 0).mean():.2%})
   - Medium Risk (1-2): {((tail_customers['CHURN_RISK_SCORE'] > 0) & (tail_customers['CHURN_RISK_SCORE'] < 3)).sum()} customers ({((tail_customers['CHURN_RISK_SCORE'] > 0) & (tail_customers['CHURN_RISK_SCORE'] < 3)).mean():.2%})
   - High Risk (3): {(tail_customers['CHURN_RISK_SCORE'] == 3).sum()} customers ({(tail_customers['CHURN_RISK_SCORE'] == 3).mean():.2%})

4. Key Findings:
   - [Add key insights based on the analysis and visualizations]

5. Recommendations:
   - [Add recommendations for addressing potential churn in tail customers]
"""

print("\nAnalysis Report:")
print(report)

# Save report to file
with open('churn_analysis_report.txt', 'w') as f:
    f.write(report)

# Save tail customers data with churn risk scores
tail_customers.to_csv('tail_customers_churn_risk.csv', index=False)

print("\nAnalysis complete. Check the generated report and CSV file for detailed results.")