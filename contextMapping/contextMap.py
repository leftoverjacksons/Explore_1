import pandas as pd
import os


def read_and_display_csv(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)

    # Display information about the DataFrame
    print("DataFrame Info:")
    print(df.info())
    print("\nColumn Names:")
    print(df.columns.tolist())

    # Display the first few rows of the DataFrame
    print("\nFirst few rows of the data:")
    print(df.head().to_string())

    # Display summary statistics
    print("\nSummary Statistics:")
    print(df.describe(include='all').to_string())


def main():
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to the CSV file
    csv_path = os.path.join(script_dir, 'DIMENSIONS.csv')

    # Check if the file exists
    if not os.path.exists(csv_path):
        print(f"Error: The file {csv_path} does not exist.")
        return

    # Read and display the CSV content
    read_and_display_csv(csv_path)


if __name__ == "__main__":
    main()