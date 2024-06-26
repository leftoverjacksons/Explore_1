import mysql.connector
from mysql.connector import Error
import configparser
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta


def read_config(config_path='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config['mysql']


def connect_to_database(db_config):
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print(f"Connected to MySQL Server on {db_config['host']}")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def perform_lifecycle_analysis(connection):
    query = """
    SELECT 
        BILL_CUSTOMER_SID,
        ORDER_DATE_SID,
        EXTENDED_PRICE
    FROM 
        F_SALES
    WHERE 
        ACTUAL_SALES_FLAG = 1
    """

    try:
        df = pd.read_sql(query, connection)

        df['ORDER_DATE'] = pd.to_datetime(df['ORDER_DATE_SID'].astype(str), format='%Y%m%d')

        # Customer Lifecycle Analysis
        customer_lifecycle = df.groupby('BILL_CUSTOMER_SID').agg({
            'ORDER_DATE': ['min', 'max', 'count'],
            'EXTENDED_PRICE': 'sum'
        }).reset_index()

        customer_lifecycle.columns = ['BILL_CUSTOMER_SID', 'FIRST_PURCHASE', 'LAST_PURCHASE', 'PURCHASE_COUNT',
                                      'TOTAL_SPEND']
        customer_lifecycle['CUSTOMER_LIFETIME_DAYS'] = (
                    customer_lifecycle['LAST_PURCHASE'] - customer_lifecycle['FIRST_PURCHASE']).dt.days

        print("\nCustomer Lifecycle Summary:")
        print(customer_lifecycle[['CUSTOMER_LIFETIME_DAYS', 'PURCHASE_COUNT', 'TOTAL_SPEND']].describe())

        plt.figure(figsize=(10, 6))
        plt.scatter(customer_lifecycle['CUSTOMER_LIFETIME_DAYS'], customer_lifecycle['TOTAL_SPEND'], alpha=0.5)
        plt.title('Customer Lifetime vs Total Spend')
        plt.xlabel('Customer Lifetime (Days)')
        plt.ylabel('Total Spend')
        plt.savefig('customer_lifetime_vs_spend.png')
        print("\nSaved plot: customer_lifetime_vs_spend.png")

        # Analyze purchase frequency
        customer_lifecycle['AVG_DAYS_BETWEEN_PURCHASES'] = customer_lifecycle['CUSTOMER_LIFETIME_DAYS'] / \
                                                           customer_lifecycle['PURCHASE_COUNT']

        print("\nAverage Days Between Purchases Summary:")
        print(customer_lifecycle['AVG_DAYS_BETWEEN_PURCHASES'].describe())

        plt.figure(figsize=(10, 6))
        plt.hist(customer_lifecycle['AVG_DAYS_BETWEEN_PURCHASES'].clip(upper=365), bins=50, edgecolor='black')
        plt.title('Distribution of Average Days Between Purchases')
        plt.xlabel('Average Days Between Purchases')
        plt.ylabel('Number of Customers')
        plt.savefig('avg_days_between_purchases.png')
        print("\nSaved plot: avg_days_between_purchases.png")

        # Identify top customers
        top_customers = customer_lifecycle.nlargest(10, 'TOTAL_SPEND')
        print("\nTop 10 Customers by Total Spend:")
        print(top_customers[['BILL_CUSTOMER_SID', 'TOTAL_SPEND', 'PURCHASE_COUNT', 'CUSTOMER_LIFETIME_DAYS',
                             'AVG_DAYS_BETWEEN_PURCHASES']])

        # Save analysis results
        customer_lifecycle.to_csv('customer_lifecycle_analysis.csv', index=False)
        print("\nSaved analysis results: customer_lifecycle_analysis.csv")

    except Error as e:
        print(f"Error performing analysis: {e}")


def main():
    db_config = read_config()
    connection = connect_to_database(db_config)
    if connection:
        perform_lifecycle_analysis(connection)
        connection.close()
        print("\nMySQL connection closed")


if __name__ == "__main__":
    main()