import mysql.connector
import matplotlib.pyplot as plt
import pandas as pd
import configparser


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
        return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None


def fetch_customer_revenue_data(connection):
    query = """
    SELECT 
        BILL_CUSTOMER_SID,
        SUM(QUANTITY_ORDERED * UNIT_PRICE - EXTENDED_COST) as revenue
    FROM 
        F_SALES
    GROUP BY 
        BILL_CUSTOMER_SID
    ORDER BY 
        revenue DESC
    """

    df = pd.read_sql(query, connection)
    return df


def plot_customer_revenue(df):
    plt.figure(figsize=(20, 10))
    plt.bar(range(len(df)), df['revenue'])
    plt.xlabel('Customers')
    plt.ylabel('Revenue')
    plt.title('Customer Revenue Distribution')
    plt.tight_layout()
    plt.savefig('customer_revenue_distribution.png', dpi=300)
    print("Graph saved as 'customer_revenue_distribution.png'")


def main():
    db_config = read_config()
    connection = connect_to_database(db_config)

    if connection:
        df = fetch_customer_revenue_data(connection)
        plot_customer_revenue(df)
        connection.close()
    else:
        print("Failed to connect to the database.")


if __name__ == "__main__":
    main()