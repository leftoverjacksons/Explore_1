import mysql.connector
from mysql.connector import Error
import configparser
from decimal import Decimal
from datetime import datetime, timedelta


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
        results = cursor.fetchall()
        return results
    except Error as e:
        print(f"Error executing query: {e}")
        return None


def get_date_ranges(months=6):
    today = datetime.now()
    end_date = today.replace(day=1)  # First day of current month
    start_date = end_date - timedelta(days=months * 30)  # Approximate 6 months ago

    return {
        'start_date': int(start_date.strftime('%Y%m%d')),
        'end_date': int(end_date.strftime('%Y%m%d'))
    }


def get_products_in_danger(connection, date_ranges, threshold=0.5, limit=20):
    query = """
    WITH sales_periods AS (
        SELECT 
            f.ITEM_SID,
            SUM(CASE WHEN f.ORDER_DATE_SID >= %(mid_date)s THEN f.QUANTITY_SHIPPED ELSE 0 END) as RECENT_QUANTITY,
            SUM(CASE WHEN f.ORDER_DATE_SID < %(mid_date)s THEN f.QUANTITY_SHIPPED ELSE 0 END) as PREVIOUS_QUANTITY,
            SUM(CASE WHEN f.ORDER_DATE_SID >= %(mid_date)s THEN f.EXTENDED_PRICE ELSE 0 END) as RECENT_REVENUE,
            SUM(CASE WHEN f.ORDER_DATE_SID < %(mid_date)s THEN f.EXTENDED_PRICE ELSE 0 END) as PREVIOUS_REVENUE
        FROM 
            F_SALES f
        WHERE 
            f.ORDER_DATE_SID BETWEEN %(start_date)s AND %(end_date)s
        GROUP BY 
            f.ITEM_SID
    )
    SELECT 
        d.ITEM_NUMBER,
        d.ITEM_DESCRIPTION,
        sp.RECENT_QUANTITY,
        sp.PREVIOUS_QUANTITY,
        sp.RECENT_REVENUE,
        sp.PREVIOUS_REVENUE,
        CASE 
            WHEN sp.PREVIOUS_QUANTITY > 0 THEN 
                (sp.RECENT_QUANTITY - sp.PREVIOUS_QUANTITY) / sp.PREVIOUS_QUANTITY * 100 
            ELSE NULL 
        END as QUANTITY_CHANGE_PERCENT,
        CASE 
            WHEN sp.PREVIOUS_REVENUE > 0 THEN 
                (sp.RECENT_REVENUE - sp.PREVIOUS_REVENUE) / sp.PREVIOUS_REVENUE * 100 
            ELSE NULL 
        END as REVENUE_CHANGE_PERCENT
    FROM 
        D_ITEMS d
    JOIN 
        sales_periods sp ON d.ITEM_SID = sp.ITEM_SID
    WHERE 
        (sp.RECENT_QUANTITY < sp.PREVIOUS_QUANTITY * %(threshold)s OR sp.RECENT_REVENUE < sp.PREVIOUS_REVENUE * %(threshold)s)
        AND (sp.RECENT_QUANTITY > 0 OR sp.PREVIOUS_QUANTITY > 0)
    ORDER BY 
        QUANTITY_CHANGE_PERCENT ASC
    LIMIT %(limit)s
    """

    mid_date = date_ranges['start_date'] + (date_ranges['end_date'] - date_ranges['start_date']) // 2

    params = {
        'start_date': date_ranges['start_date'],
        'end_date': date_ranges['end_date'],
        'mid_date': mid_date,
        'threshold': threshold,
        'limit': limit
    }

    return execute_query(connection, query, params)


def get_sales_data(connection, date_ranges):
    query = """
    SELECT 
        MIN(ORDER_DATE_SID) as MIN_DATE,
        MAX(ORDER_DATE_SID) as MAX_DATE,
        COUNT(*) as TOTAL_RECORDS,
        COUNT(DISTINCT ITEM_SID) as DISTINCT_ITEMS
    FROM 
        F_SALES
    WHERE 
        ORDER_DATE_SID BETWEEN %(start_date)s AND %(end_date)s
    """

    return execute_query(connection, query, date_ranges)


def main():
    db_config = read_config()
    connection = connect_to_database(db_config)

    if connection:
        date_ranges = get_date_ranges(months=12)  # Analyze last 12 months
        print(f"\nAnalyzing data from {date_ranges['start_date']} to {date_ranges['end_date']}")

        sales_data = get_sales_data(connection, date_ranges)
        if sales_data:
            print("\nSales Data Summary:")
            print(f"Date Range: {sales_data[0]['MIN_DATE']} to {sales_data[0]['MAX_DATE']}")
            print(f"Total Records: {sales_data[0]['TOTAL_RECORDS']}")
            print(f"Distinct Items: {sales_data[0]['DISTINCT_ITEMS']}")

        at_risk_products = get_products_in_danger(connection, date_ranges, threshold=0.7, limit=50)

        if at_risk_products:
            print("\nProducts Potentially at Risk:")
            print("{:<15} {:<40} {:<15} {:<15} {:<15} {:<15}".format(
                "Item Number", "Description", "Qty Change %", "Rev Change %", "Recent Qty", "Recent Revenue"))
            print("-" * 115)
            for product in at_risk_products:
                print("{:<15} {:<40} {:<15.2f} {:<15.2f} {:<15} ${:<14.2f}".format(
                    product['ITEM_NUMBER'],
                    product['ITEM_DESCRIPTION'][:37] + '...' if len(product['ITEM_DESCRIPTION']) > 40 else product[
                        'ITEM_DESCRIPTION'],
                    product['QUANTITY_CHANGE_PERCENT'] or 0,
                    product['REVENUE_CHANGE_PERCENT'] or 0,
                    product['RECENT_QUANTITY'] or 0,
                    Decimal(product['RECENT_REVENUE'] or 0)
                ))
        else:
            print("\nNo at-risk products found based on current criteria.")
            print("Consider adjusting the threshold or date range further.")

        connection.close()
        print("\nMySQL connection closed")


if __name__ == "__main__":
    main()