import configparser
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_config(config_path='../config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def create_db_engine(config):
    return create_engine(
        f"mysql+mysqlconnector://{config['mysql']['user']}:{config['mysql']['password']}@{config['mysql']['host']}/{config['mysql']['database']}")


def fetch_sales_data(engine):
    query = """
    SELECT 
        BILL_CUSTOMER_SID, 
        EXTENDED_PRICE
    FROM 
        F_SALES
    """
    return pd.read_sql(query, engine)


def elbow_method(data):
    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(data.reshape(-1, 1))

    inertias = []
    for k in range(1, 11):
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(data_scaled)
        inertias.append(kmeans.inertia_)

    # Plot the elbow curve
    plt.figure(figsize=(10, 6))
    plt.plot(range(1, 11), inertias, marker='o')
    plt.xlabel('Number of clusters (k)')
    plt.ylabel('Inertia')
    plt.title('Elbow Method for Optimal k')
    plt.savefig('elbow_curve.png')
    plt.close()

    # Find the elbow point
    diffs = np.diff(inertias)
    elbow_point = np.argmax(diffs) + 1

    return elbow_point


def kmeans_segmentation(df, n_clusters):
    customer_summary = df.groupby('BILL_CUSTOMER_SID')['EXTENDED_PRICE'].sum().reset_index()
    customer_summary.columns = ['CUSTOMER_ID', 'TOTAL_REVENUE']

    scaler = StandardScaler()
    revenue_scaled = scaler.fit_transform(customer_summary['TOTAL_REVENUE'].values.reshape(-1, 1))

    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    customer_summary['CLUSTER'] = kmeans.fit_predict(revenue_scaled)

    # Sort clusters by mean revenue
    cluster_order = customer_summary.groupby('CLUSTER')['TOTAL_REVENUE'].mean().sort_values(ascending=False).index
    cluster_mapping = {cluster: f'Segment {i + 1}' for i, cluster in enumerate(cluster_order)}
    customer_summary['SEGMENT'] = customer_summary['CLUSTER'].map(cluster_mapping)

    return customer_summary


def print_summary(customer_summary):
    total_revenue = customer_summary['TOTAL_REVENUE'].sum()
    logging.info("\nCustomer Segmentation Summary:")
    logging.info(f"Total customers: {len(customer_summary)}")

    for segment in sorted(customer_summary['SEGMENT'].unique()):
        segment_data = customer_summary[customer_summary['SEGMENT'] == segment]
        segment_revenue = segment_data['TOTAL_REVENUE'].sum()

        logging.info(f"\n{segment}:")
        logging.info(f"Customers: {len(segment_data)} ({len(segment_data) / len(customer_summary):.2%})")
        logging.info(f"Revenue: ${segment_revenue:,.2f} ({segment_revenue / total_revenue:.2%})")
        logging.info(f"Average revenue per customer: ${segment_revenue / len(segment_data):,.2f}")


def plot_revenue_distribution(customer_summary):
    plt.figure(figsize=(12, 6))
    for segment in sorted(customer_summary['SEGMENT'].unique()):
        segment_data = customer_summary[customer_summary['SEGMENT'] == segment]
        plt.hist(segment_data['TOTAL_REVENUE'], bins=50, alpha=0.5, label=segment)
    plt.title('Distribution of Customer Revenue by Segment')
    plt.xlabel('Total Revenue')
    plt.ylabel('Number of Customers')
    plt.legend()
    plt.savefig('revenue_distribution_by_segment.png')
    plt.close()


def main():
    try:
        config = load_config()
        engine = create_db_engine(config)
        df = fetch_sales_data(engine)

        # K-means Segmentation
        optimal_k = elbow_method(df.groupby('BILL_CUSTOMER_SID')['EXTENDED_PRICE'].sum().values)
        logging.info(f"\nOptimal number of clusters based on Elbow Method: {optimal_k}")

        kmeans_summary = kmeans_segmentation(df, optimal_k)
        logging.info("\nK-means Segmentation:")
        print_summary(kmeans_summary)

        # Plot revenue distribution
        plot_revenue_distribution(kmeans_summary)

        # Save results
        kmeans_summary.to_csv('kmeans_segmentation.csv', index=False)
        logging.info("\nAnalysis complete. Results saved to CSV file.")
        logging.info("Elbow curve plot saved as 'elbow_curve.png'.")
        logging.info("Revenue distribution plot saved as 'revenue_distribution_by_segment.png'.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()