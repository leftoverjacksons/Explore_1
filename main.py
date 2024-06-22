import mysql.connector
from mysql.connector import Error


def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='your_host',
            port=3306,
            database='your_database',
            user='your_username',
            password='your_password'
        )

        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            print(f"You're connected to database: {record[0]}")

            return connection

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None


def fetch_data(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except Error as e:
        print(f"Error executing query: {e}")
        return None


def main():
    connection = connect_to_database()
    if connection is not None:
        # Example query - replace with your actual query
        query = "SELECT * FROM your_table LIMIT 5"
        results = fetch_data(connection, query)

        if results:
            for row in results:
                print(row)

        connection.close()
        print("MySQL connection is closed")


if __name__ == "__main__":
    main()