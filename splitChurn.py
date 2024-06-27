import os
import sys
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import configparser

# ... (keep the config reading part from the previous script) ...

try:
    engine = create_engine(
        f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")

    # Inspect table structure
    with engine.connect() as connection:
        result = connection.execute("DESCRIBE F_SALES_QUOTES")
        print("F_SALES_QUOTES table structure:")
        for row in result:
            print(row)

        # Get a sample of data
        result = connection.execute("SELECT * FROM F_SALES_QUOTES LIMIT 5")
        print("\nSample data from F_SALES_QUOTES:")
        for row in result:
            print(row)

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

print("Inspection complete.")