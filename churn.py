import mysql.connector
from mysql.connector import Error
import configparser
import pandas as pd

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

def export_sample_to_csv(connection, filename='f_sales_sample.csv', limit=100):
    query = f"SELECT * FROM F_SALES LIMIT {limit}"
    try:
        df = pd.read_sql(query, connection)
        df.to_csv(filename, index=False)
        print(f"Exported {len(df)} rows to {filename}")
    except Error as e:
        print(f"Error exporting data: {e}")

def main():
    db_config = read_config()
    connection = connect_to_database(db_config)
    if connection:
        export_sample_to_csv(connection)
        connection.close()
        print("MySQL connection closed")

if __name__ == "__main__":
    main()