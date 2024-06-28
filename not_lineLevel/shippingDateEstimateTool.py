import configparser
import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def read_db_config(filename='../config.ini', section='mysql'):
    parser = configparser.ConfigParser()
    parser.read(filename)
    return {k: v for k, v in parser.items(section)}

def connect_to_database(db_config):
    try:
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            print(f"Connected to MySQL Server on {db_config['host']}")
        return conn
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def fetch_all_sales_data(conn):
    query = """
    SELECT 
        F.ORDER_DATE_SID, 
        F.ACTUAL_SHIP_DATE_SID, 
        F.QUANTITY_ORDERED, 
        F.ITEM_SID,
        F.SHIP_CUSTOMER_SID,
        D.ITEM_CATEGORY_SID,
        D.ITEM_FAMILY_SID
    FROM 
        F_SALES F
    JOIN
        D_ITEMS D ON F.ITEM_SID = D.ITEM_SID
    WHERE 
        F.ACTUAL_SHIP_DATE_SID IS NOT NULL
    ORDER BY 
        F.ORDER_DATE_SID DESC
    LIMIT 100000
    """
    print("Fetching data from MySQL...")
    df = pd.read_sql(query, conn)
    print(f"Fetched {len(df)} rows of data.")
    return df

def preprocess_data(df):
    print("Preprocessing data...")
    df['ORDER_DATE'] = pd.to_datetime(df['ORDER_DATE_SID'], format='%Y%m%d')
    df['ACTUAL_SHIP_DATE'] = pd.to_datetime(df['ACTUAL_SHIP_DATE_SID'], format='%Y%m%d')
    df['DAYS_TO_SHIP'] = (df['ACTUAL_SHIP_DATE'] - df['ORDER_DATE']).dt.days
    return df

def calculate_item_statistics(df):
    item_stats = df.groupby('ITEM_SID')['DAYS_TO_SHIP'].agg(['mean', 'median']).reset_index()
    overall_mean = df['DAYS_TO_SHIP'].mean()
    overall_median = df['DAYS_TO_SHIP'].median()
    print(f"Overall Mean Days to Ship: {overall_mean}")
    print(f"Overall Median Days to Ship: {overall_median}")
    return item_stats, overall_mean, overall_median

def estimate_shipping_date(item_stats, overall_median, item_sid, order_date):
    item_stat = item_stats[item_stats['ITEM_SID'] == item_sid]
    if not item_stat.empty:
        estimated_days = item_stat['median'].values[0]
    else:
        estimated_days = overall_median
    return order_date + timedelta(days=estimated_days), estimated_days

def get_item_details(conn, item_sid):
    query = f"""
    SELECT ITEM_NUMBER, ITEM_DESCRIPTION, ITEM_CATEGORY_SID, ITEM_FAMILY_SID
    FROM D_ITEMS
    WHERE ITEM_SID = {item_sid}
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    return result

def sanity_check(df, item_sid):
    item_data = df[df['ITEM_SID'] == item_sid]
    if item_data.empty:
        return "No data available for this item"

    min_days = item_data['DAYS_TO_SHIP'].min()
    max_days = item_data['DAYS_TO_SHIP'].max()
    mean_days = item_data['DAYS_TO_SHIP'].mean()
    median_days = item_data['DAYS_TO_SHIP'].median()

    return f"Sanity Check for Item SID {item_sid}:\n" \
           f"Minimum days to ship: {min_days}\n" \
           f"Maximum days to ship: {max_days}\n" \
           f"Mean days to ship: {mean_days:.2f}\n" \
           f"Median days to ship: {median_days}"

def main():
    try:
        db_config = read_db_config()
        conn = connect_to_database(db_config)
        if conn:
            df = fetch_all_sales_data(conn)
            df = preprocess_data(df)

            item_stats, overall_mean, overall_median = calculate_item_statistics(df)

            # Example customer and order details
            item_sid = 32599
            order_date = datetime.now()

            item_details = get_item_details(conn, item_sid)

            estimated_date, estimated_days = estimate_shipping_date(
                item_stats, overall_median, item_sid, order_date
            )

            print("\nOrder Details:")
            print(f"Item: {item_details['ITEM_NUMBER']} - {item_details['ITEM_DESCRIPTION']}")
            print(f"Order Date: {order_date.strftime('%Y-%m-%d')}")
            print(f"\nEstimated shipping date: {estimated_date.strftime('%Y-%m-%d')}")
            print(f"Estimated days to ship: {estimated_days}")

            print("\n" + sanity_check(df, item_sid))

            conn.close()
            print("\nMySQL connection closed")
    except ValueError as ve:
        print(f"Value Error: {ve}")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
