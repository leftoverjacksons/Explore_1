import mysql.connector
from mysql.connector import Error
import configparser
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt


def read_config(config_path='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config['mysql']


def connect_to_database(db_config):
    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        if connection.is_connected():
            print(f"Connected to MySQL Server on {db_config['host']}")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def execute_query(connection, query, params=None):
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params or ())
        return cursor.fetchall()
    except Error as e:
        print(f"Error executing query: {e}")
        return None


def analyze_churn(connection):
    # Define the churn period (e.g., 3 months)
    churn_period = 3

    # Query to get customer activity
    query = """
    WITH customer_activity AS (
        SELECT 
            f.BILL_CUSTOMER_SID,
            DATE(d.DOCUMENT_DATE_SID) AS activity_date,
            SUM(f.EXTENDED_PRICE) AS total_sales
        FROM 
            F_SALES f
        JOIN 
            D_SALES_DOCUMENTS d ON f.SALES_DOCUMENT_SID = d.SALES_DOCUMENT_SID
        GROUP BY 
            f.BILL_CUSTOMER_SID, DATE(d.DOCUMENT_DATE_SID)
    ),
    customer_last_activity AS (
        SELECT 
            BILL_CUSTOMER_SID,
            MAX(activity_date) AS last_activity_date
        FROM 
            customer_activity
        GROUP BY 
            BILL_CUSTOMER_SID
    )
    SELECT 
        cla.BILL_CUSTOMER_SID,
        cla.last_activity_date,
        DATEDIFF(CURDATE(), cla.last_activity_date) AS days_since_last_activity
    FROM 
        customer_last_activity cla
    ORDER BY 
        days_since_last_activity DESC;
    """

    results = execute_query(connection, query)

    if not results:
        print("No data found for churn analysis.")
        return

    # Convert results to a pandas DataFrame
    df = pd.DataFrame(results)

    # Calculate churn
    current_date = datetime.now().date()
    df['is_churned'] = df['days_since_last_activity'] > (churn_period * 30)

    # Basic churn metrics
    total_customers = len(df)
    churned_customers = df['is_churned'].sum()
    churn_rate = churned_customers / total_customers

    print(f"\nChurn Analysis Results:")
    print(f"Total Customers: {total_customers}")
    print(f"Churned Customers: {churned_customers}")
    print(f"Churn Rate: {churn_rate:.2%}")

    # Visualize churn
    plt.figure(figsize=(10, 6))
    plt.hist(df['days_since_last_activity'], bins=50, edgecolor='black')
    plt.title('Distribution of Days Since Last Activity')
    plt.xlabel('Days')
    plt.ylabel('Number of Customers')
    plt.axvline(x=churn_period * 30, color='r', linestyle='--', label=f'Churn Threshold ({churn_period} months)')
    plt.legend()
    plt.savefig('churn_distribution.png')
    print(f"\nChurn distribution plot saved as 'churn_distribution.png'")

    # Top 10 churned customers
    top_churned = df[df['is_churned']].nlargest(10, 'days_since_last_activity')
    print("\nTop 10 Churned Customers:")
    print(top_churned[['BILL_CUSTOMER_SID', 'last_activity_date', 'days_since_last_activity']])


def main():
    try:
        db_config = read_config()
        connection = connect_to_database(db_config)
        if connection:
            analyze_churn(connection)
            connection.close()
            print("\nMySQL connection closed")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()