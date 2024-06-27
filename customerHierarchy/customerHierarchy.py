import mysql.connector
from mysql.connector import Error
import pandas as pd
import configparser
from collections import defaultdict
import csv


def read_db_config(filename='../config.ini', section='mysql'):
    parser = configparser.ConfigParser()
    parser.read(filename)
    return {k: v for k, v in parser.items(section)}


def connect_to_db():
    db_config = read_db_config()
    connection = None
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Connected to the database")
    except Error as e:
        print(f"Error: {e}")
    return connection


def fetch_sales_data(connection):
    query = "SELECT BILL_CUSTOMER_SID, SHIP_CUSTOMER_SID FROM F_SALES"
    df = pd.read_sql(query, connection)
    return df


def build_hierarchy(df):
    hierarchy = defaultdict(list)
    for _, row in df.iterrows():
        bill_customer = row['BILL_CUSTOMER_SID']
        ship_customer = row['SHIP_CUSTOMER_SID']
        if pd.notna(bill_customer) and pd.notna(ship_customer):
            hierarchy[bill_customer].append(ship_customer)
    return hierarchy


def save_hierarchy_to_csv(hierarchy, filename='customer_hierarchy.csv'):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['ParentCustomer', 'ChildCustomer'])
        for parent, children in hierarchy.items():
            for child in children:
                writer.writerow([parent, child])


def main():
    connection = connect_to_db()
    if connection is not None:
        df = fetch_sales_data(connection)
        connection.close()

        if df.empty:
            print("No data found in F_SALES.")
        else:
            hierarchy = build_hierarchy(df)
            save_hierarchy_to_csv(hierarchy)
            print(f"Customer hierarchy saved to customer_hierarchy.csv")
    else:
        print("Failed to connect to the database")


if __name__ == "__main__":
    main()
