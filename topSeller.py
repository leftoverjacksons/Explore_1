import mysql.connector
from mysql.connector import Error
import configparser
from decimal import Decimal


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


def get_top_selling_items(connection, limit=10):
    query = """
    SELECT 
        d.ITEM_NUMBER,
        d.ITEM_DESCRIPTION,
        SUM(f.QUANTITY_SHIPPED) as TOTAL_QUANTITY,
        SUM(f.EXTENDED_PRICE) as TOTAL_REVENUE
    FROM 
        F_SALES f
    JOIN 
        D_ITEMS d ON f.ITEM_SID = d.ITEM_SID
    GROUP BY 
        d.ITEM_NUMBER, d.ITEM_DESCRIPTION
    ORDER BY 
        TOTAL_QUANTITY DESC
    LIMIT %s
    """
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        return results
    except Error as e:
        print(f"Error executing query: {e}")
        return None


def main():
    db_config = read_config()
    connection = connect_to_database(db_config)

    if connection:
        top_items = get_top_selling_items(connection)

        if top_items:
            print("\nTop Selling Items:")
            print("{:<15} {:<50} {:<15} {:<15}".format(
                "Item Number", "Description", "Total Quantity", "Total Revenue"))
            print("-" * 95)
            for item in top_items:
                print("{:<15} {:<50} {:<15} ${:<14.2f}".format(
                    item['ITEM_NUMBER'],
                    item['ITEM_DESCRIPTION'][:47] + '...' if len(item['ITEM_DESCRIPTION']) > 50 else item[
                        'ITEM_DESCRIPTION'],
                    item['TOTAL_QUANTITY'],
                    Decimal(item['TOTAL_REVENUE'])
                ))
        else:
            print("No data found or error occurred.")

        connection.close()
        print("\nMySQL connection closed")


if __name__ == "__main__":
    main()