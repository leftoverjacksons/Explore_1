import mysql.connector
from mysql.connector import Error
import pandas as pd
import configparser
from collections import defaultdict
import sqlite3


def read_db_config(filename='../config.ini', section='mysql'):
    parser = configparser.ConfigParser()
    parser.read(filename)
    return {k: v for k, v in parser.items(section)}


def connect_to_db():
    db_config = read_db_config()
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Connected to the database")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None


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


def save_hierarchy_to_db(hierarchy, db_filename='customer_hierarchy.db'):
    conn = sqlite3.connect(db_filename)
    c = conn.cursor()
    c.execute('DROP TABLE IF EXISTS customer_hierarchy')
    c.execute('CREATE TABLE customer_hierarchy (parent_customer INTEGER, child_customer INTEGER)')

    for parent, children in hierarchy.items():
        for child in children:
            c.execute('INSERT INTO customer_hierarchy (parent_customer, child_customer) VALUES (?, ?)', (parent, child))

    conn.commit()
    conn.close()
    print(f"Customer hierarchy saved to {db_filename}")


def query_hierarchy(parent_customer, db_filename='customer_hierarchy.db'):
    conn = sqlite3.connect(db_filename)
    c = conn.cursor()
    c.execute('SELECT child_customer FROM customer_hierarchy WHERE parent_customer = ?', (parent_customer,))
    children = c.fetchall()
    conn.close()
    return [child[0] for child in children]


def main():
    connection = connect_to_db()
    if connection is not None:
        df = fetch_sales_data(connection)
        connection.close()

        if df.empty:
            print("No data found in F_SALES.")
        else:
            hierarchy = build_hierarchy(df)
            save_hierarchy_to_db(hierarchy)

            # Example query
            parent_customer = 12345  # Replace with actual customer SID to query
            children = query_hierarchy(parent_customer)
            print(f"Children of customer {parent_customer}: {children}")
    else:
        print("Failed to connect to the database")


if __name__ == "__main__":
    main()
