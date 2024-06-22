import mysql.connector
from mysql.connector import Error
import configparser
import os


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


def explore_schema(connection, schema_name):
    print(f"\n=== Exploring Schema: {schema_name} ===")

    # Switch to this schema
    connection.database = schema_name

    # Check for tables (including hidden ones)
    tables = execute_query(connection, "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
    if tables:
        print("Tables:")
        for table in tables:
            print(f"  {table['Tables_in_' + schema_name]}")
    else:
        print("No tables found.")

    # Check for views
    views = execute_query(connection, "SHOW FULL TABLES WHERE Table_type = 'VIEW'")
    if views:
        print("\nViews:")
        for view in views:
            print(f"  {view['Tables_in_' + schema_name]}")
    else:
        print("\nNo views found.")

    # Check for stored procedures
    procedures = execute_query(connection, "SHOW PROCEDURE STATUS WHERE Db = %s", (schema_name,))
    if procedures:
        print("\nStored Procedures:")
        for proc in procedures:
            print(f"  {proc['Name']}")
    else:
        print("\nNo stored procedures found.")

    # Check for functions
    functions = execute_query(connection, "SHOW FUNCTION STATUS WHERE Db = %s", (schema_name,))
    if functions:
        print("\nFunctions:")
        for func in functions:
            print(f"  {func['Name']}")
    else:
        print("\nNo functions found.")

    # Check for triggers
    triggers = execute_query(connection, "SHOW TRIGGERS")
    if triggers:
        print("\nTriggers:")
        for trigger in triggers:
            print(f"  {trigger['Trigger']}")
    else:
        print("\nNo triggers found.")


def main():
    db_config = read_config()
    connection = connect_to_database(db_config)
    if connection:
        explore_schema(connection, db_config['database'])
        connection.close()
        print("\nMySQL connection closed")


if __name__ == "__main__":
    main()