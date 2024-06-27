import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import configparser
import logging
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_config(config_path='../config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def create_db_engine(config):
    mysql_config = config['mysql']
    return create_engine(
        f"mysql+mysqlconnector://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
    )


def load_data(engine):
    query = """
    SELECT BILL_CUSTOMER_SID, ORDER_DATE_SID, EXTENDED_PRICE
    FROM F_SALES
    WHERE ORDER_DATE_SID >= DATE_SUB(CURDATE(), INTERVAL 2 YEAR)
    """
    return pd.read_sql(query, engine)


def prepare_features(data):
    data['ORDER_DATE'] = pd.to_datetime(data['ORDER_DATE_SID'], format='%Y%m%d')

    # Calculate RFM
    current_date = pd.Timestamp.now()
    rfm = data.groupby('BILL_CUSTOMER_SID').agg({
        'ORDER_DATE': lambda x: (current_date - x.max()).days,
        'ORDER_DATE_SID': 'count',
        'EXTENDED_PRICE': 'sum'
    }).rename(columns={
        'ORDER_DATE': 'recency',
        'ORDER_DATE_SID': 'frequency',
        'EXTENDED_PRICE': 'monetary'
    })

    # Calculate 3-month rolling averages
    data['YEAR_MONTH'] = data['ORDER_DATE'].dt.to_period('M')
    monthly_data = data.groupby(['BILL_CUSTOMER_SID', 'YEAR_MONTH']).agg({
        'ORDER_DATE_SID': 'count',
        'EXTENDED_PRICE': 'sum'
    }).reset_index()

    monthly_data = monthly_data.sort_values(['BILL_CUSTOMER_SID', 'YEAR_MONTH'])
    monthly_data['rolling_freq'] = monthly_data.groupby('BILL_CUSTOMER_SID')['ORDER_DATE_SID'].rolling(window=3,
                                                                                                       min_periods=1).mean().reset_index(
        0, drop=True)
    monthly_data['rolling_monetary'] = monthly_data.groupby('BILL_CUSTOMER_SID')['EXTENDED_PRICE'].rolling(window=3,
                                                                                                           min_periods=1).mean().reset_index(
        0, drop=True)

    # Calculate trend
    monthly_data['freq_trend'] = monthly_data.groupby('BILL_CUSTOMER_SID')['ORDER_DATE_SID'].diff()
    monthly_data['monetary_trend'] = monthly_data.groupby('BILL_CUSTOMER_SID')['EXTENDED_PRICE'].diff()

    # Get the most recent values for each customer
    recent_behavior = monthly_data.groupby('BILL_CUSTOMER_SID').last().reset_index()

    # Merge RFM and recent behavior
    features = rfm.merge(
        recent_behavior[['BILL_CUSTOMER_SID', 'rolling_freq', 'rolling_monetary', 'freq_trend', 'monetary_trend']],
        on='BILL_CUSTOMER_SID', how='left')

    return features


def cluster_customers(features):
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features.drop('BILL_CUSTOMER_SID', axis=1))

    kmeans = KMeans(n_clusters=5, random_state=42)
    features['cluster'] = kmeans.fit_predict(scaled_features)

    return features


def plot_cluster_characteristics(features):
    plt.figure(figsize=(15, 10))
    for i in range(5):
        plt.subplot(2, 3, i + 1)
        cluster_data = features[features['cluster'] == i]
        sns.scatterplot(data=cluster_data, x='recency', y='frequency', size='monetary', hue='monetary_trend',
                        palette='viridis')
        plt.title(f'Cluster {i}')
    plt.tight_layout()
    plt.savefig('cluster_characteristics.png')
    plt.close()


def main():
    try:
        config = load_config()
        logging.info("Configuration loaded successfully")

        engine = create_db_engine(config)
        logging.info("Database engine created successfully")

        data = load_data(engine)
        logging.info(f"Data loaded. Shape: {data.shape}")

        features = prepare_features(data)
        logging.info(f"Features prepared. Shape: {features.shape}")

        clustered_features = cluster_customers(features)
        logging.info("Customers clustered")

        plot_cluster_characteristics(clustered_features)
        logging.info("Cluster characteristics plot saved")

        # Identify customers with negative trends
        at_risk_customers = clustered_features[
            (clustered_features['freq_trend'] < 0) &
            (clustered_features['monetary_trend'] < 0)
            ]
        logging.info(f"Number of at-risk customers: {len(at_risk_customers)}")

        # Save results
        clustered_features.to_csv('customer_behavior_analysis.csv', index=False)
        at_risk_customers.to_csv('at_risk_customers.csv', index=False)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    main()