import configparser
import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import cross_val_score
from sqlalchemy import create_engine


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
    LIMIT 100000  # Adjust this limit based on your data size and memory constraints
    """
    print("Fetching data from MySQL...")
    df = pd.read_sql(query, conn)
    print(f"Fetched {len(df)} rows of data.")
    return df


class CustomLabelEncoder(LabelEncoder):
    def fit(self, y):
        super().fit(y)
        self.classes_ = np.append(self.classes_, 'unseen')
        return self

    def transform(self, y):
        try:
            return super().transform(y)
        except ValueError:
            return len(self.classes_) - 1  # Return index of 'unseen'


def preprocess_data(df):
    print("Preprocessing data...")
    df['ORDER_DATE'] = pd.to_datetime(df['ORDER_DATE_SID'], format='%Y%m%d')
    df['ACTUAL_SHIP_DATE'] = pd.to_datetime(df['ACTUAL_SHIP_DATE_SID'], format='%Y%m%d')
    df['DAYS_TO_SHIP'] = (df['ACTUAL_SHIP_DATE'] - df['ORDER_DATE']).dt.days

    df['MONTH'] = df['ORDER_DATE'].dt.month
    df['DAY_OF_WEEK'] = df['ORDER_DATE'].dt.weekday

    le_dict = {}
    for col in ['ITEM_CATEGORY_SID', 'ITEM_FAMILY_SID', 'SHIP_CUSTOMER_SID']:
        le = CustomLabelEncoder()
        df[f'{col}_ENCODED'] = le.fit_transform(df[col])
        le_dict[col] = le

    print("Data preprocessing complete.")
    return df, le_dict


def train_model(df):
    print("Training model...")
    features = ['QUANTITY_ORDERED', 'ITEM_CATEGORY_SID_ENCODED', 'ITEM_FAMILY_SID_ENCODED', 'SHIP_CUSTOMER_SID_ENCODED',
                'MONTH', 'DAY_OF_WEEK']
    X = df[features]
    y = df['DAYS_TO_SHIP']

    model = RandomForestRegressor(n_estimators=100, random_state=42)

    # Use cross-validation
    cv_scores = cross_val_score(model, X, y, cv=5)
    print(f"Cross-validation scores: {cv_scores}")
    print(f"Mean CV score: {np.mean(cv_scores)}")

    # Fit the model on all data
    model.fit(X, y)
    print("Model training complete.")
    return model


def estimate_shipping_date(model, le_dict, quantity, item_category, item_family, customer_sid, order_date):
    month = order_date.month
    day_of_week = order_date.weekday()
    item_category_encoded = le_dict['ITEM_CATEGORY_SID'].transform([item_category])[0]
    item_family_encoded = le_dict['ITEM_FAMILY_SID'].transform([item_family])[0]
    customer_encoded = le_dict['SHIP_CUSTOMER_SID'].transform([customer_sid])[0]

    prediction = model.predict(
        [[quantity, item_category_encoded, item_family_encoded, customer_encoded, month, day_of_week]])
    estimated_days = max(1, int(round(prediction[0])))  # Ensure at least 1 day
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


def main():
    try:
        db_config = read_db_config()
        conn = connect_to_database(db_config)
        if conn:
            # Fetch all sales data
            df = fetch_all_sales_data(conn)
            df, le_dict = preprocess_data(df)

            # Train model
            model = train_model(df)

            # Example customer and order details
            customer_sid = 7850
            item_sid = 57001
            quantity = 10
            order_date = datetime.now()

            # Get item details
            item_details = get_item_details(conn, item_sid)

            # Estimate shipping date
            estimated_date, estimated_days = estimate_shipping_date(
                model, le_dict, quantity,
                item_details['ITEM_CATEGORY_SID'],
                item_details['ITEM_FAMILY_SID'],
                customer_sid,
                order_date
            )

            print("\nOrder Details:")
            print(f"Customer SID: {customer_sid}")
            print(f"Item: {item_details['ITEM_NUMBER']} - {item_details['ITEM_DESCRIPTION']}")
            print(f"Quantity: {quantity}")
            print(f"Order Date: {order_date.strftime('%Y-%m-%d')}")
            print(f"\nEstimated shipping date: {estimated_date.strftime('%Y-%m-%d')}")
            print(f"Estimated days to ship: {estimated_days}")

            conn.close()
            print("\nMySQL connection closed")
    except ValueError as ve:
        print(f"Value Error: {ve}")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        print(traceback.format_exc())  # This will print the full traceback


if __name__ == "__main__":
    main()