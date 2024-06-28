# Shipping Date Estimate Tool

## Overview

The `shippingDateEstimateTool.py` script is designed to estimate shipping lead times based on historical sales data. This tool calculates the expected shipping date for a given item based on historical shipping times and provides statistical insights for each item. The script uses a simplified statistical approach to provide quick and reasonably accurate estimates.

## Features

- **Fetch Data from MySQL**: Connects to a MySQL database and fetches historical sales data.
- **Data Preprocessing**: Converts date fields and calculates the number of days taken to ship each order.
- **Item Statistics Calculation**: Computes mean and median shipping times for each item as well as overall statistics.
- **Estimate Shipping Date**: Uses the calculated statistics to estimate the shipping date for a given item.
- **Sanity Check**: Provides minimum, maximum, mean, and median shipping times for the specified item.

## Dependencies

- `configparser`
- `mysql-connector-python`
- `pandas`
- `numpy`
- `datetime`
- `matplotlib`

## Installation

1. Install the required Python packages using pip:
    ```sh
    pip install configparser mysql-connector-python pandas numpy matplotlib
    ```

2. Ensure you have access to a MySQL database with the necessary tables and data.

## Configuration

The script reads database connection details from a configuration file named `config.ini`. Ensure you have a `config.ini` file in the same directory as the script with the following format:

```ini
[mysql]
host = your_host
database = your_database
user = your_user
password = your_password
