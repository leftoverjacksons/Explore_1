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


def get_top_products_by_revenue(connection, limit=20):
    query = """
    SELECT 
        d.ITEM_NUMBER,
        d.ITEM_DESCRIPTION,
        SUM(f.QUANTITY_SHIPPED) as TOTAL_QUANTITY,
        SUM(f.EXTENDED_PRICE) as TOTAL_REVENUE,
        AVG(f.UNIT_PRICE) as AVG_UNIT_PRICE,
        SUM(f.EXTENDED_COST) as TOTAL_COST,
        SUM(f.EXTENDED_PRICE) - SUM(f.EXTENDED_COST) as GROSS_PROFIT,
        MIN(f.ORDER_DATE_SID) as FIRST_ORDER_DATE,
        MAX(f.ORDER_DATE_SID) as LAST_ORDER_DATE
    FROM 
        F_SALES f
    JOIN 
        D_ITEMS d ON f.ITEM_SID = d.ITEM_SID
    GROUP BY 
        d.ITEM_NUMBER, d.ITEM_DESCRIPTION
    ORDER BY 
        TOTAL_REVENUE DESC
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
        top_products = get_top_products_by_revenue(connection)

        if top_products:
            print("\nTop Products by Revenue:")
            print("{:<15} {:<30} {:<15} {:<15} {:<15} {:<15} {:<15} {:<15} {:<15}".format(
                "Item Number", "Description", "Total Quantity", "Total Revenue", "Avg Unit Price",
                "Total Cost", "Gross Profit", "First Order", "Last Order"))
            print("-" * 150)
            for product in top_products:
                print("{:<15} {:<30} {:<15} ${:<14.2f} ${:<14.2f} ${:<14.2f} ${:<14.2f} {:<15} {:<15}".format(
                    product['ITEM_NUMBER'],
                    product['ITEM_DESCRIPTION'][:27] + '...' if len(product['ITEM_DESCRIPTION']) > 30 else product[
                        'ITEM_DESCRIPTION'],
                    product['TOTAL_QUANTITY'],
                    Decimal(product['TOTAL_REVENUE']),
                    Decimal(product['AVG_UNIT_PRICE']),
                    Decimal(product['TOTAL_COST']) if product['TOTAL_COST'] is not None else Decimal('0.00'),
                    Decimal(product['GROSS_PROFIT']) if product['GROSS_PROFIT'] is not None else Decimal('0.00'),
                    str(product['FIRST_ORDER_DATE']),
                    str(product['LAST_ORDER_DATE'])
                ))
        else:
            print("No data found or error occurred.")

        connection.close()
        print("\nMySQL connection closed")


if __name__ == "__main__":
    main()