
#%% Step 1: Fetch Data from API

# Import the requests library for handling HTTP requests
import requests

# Define the API endpoint URL for LinkedIn job scraping
url = "https://linkedin-data-scraper.p.rapidapi.com/search_jobs"

# Payload contains the parameters required for the API request
payload = {
    "keywords": "Data Engineer",  # Job title to search for
    "location": "London, United KIngdom",  # Location of the job
    "count": 50  # Number of job results to retrieve
}

# Headers include the API key and host for authentication and identification
headers = {
    "x-rapidapi-key": "",  # Your API key
    "x-rapidapi-host": "linkedin-data-scraper.p.rapidapi.com",  # Host for the API
    "Content-Type": "application/json"  # Data type for the request body
}

# Make a POST request to the API with the payload and headers
response = requests.post(url, json=payload, headers=headers)

# Print the raw JSON response to inspect the data structure
print(response.json())

######################################################################################################

#%% Step 2: Transform API Response into a Usable Format

# Import the pandas library for data manipulation
import pandas as pd

# Parse the JSON data from the API response
json_data = response.json()

# Try block for error handling during transformation
try:
    # Check if 'response' exists in the JSON data and ensure it is a list
    if 'response' in json_data and isinstance(json_data['response'], list):
        main = []

        # Flatten nested JSON into a single list
        for item in json_data['response']:
            if isinstance(item, list):  # If the item is a list, extend the main list
                main.extend(item)
            elif isinstance(item, dict):  # If the item is a dictionary, add it to the list
                main.append(item)

        # Convert the flattened list into a pandas DataFrame
        df = pd.DataFrame(main)

        # Rename columns for better readability and clarity
        column_to_rename = {
            'title': 'JobTitle',
            'comapnyURL1': 'CompanyURL1',
            'comapnyURL2': 'CompanyURL2',
            'companyId': 'CompanyId',
            'companyUniversalName': 'CompanyUniversalName',
            'companyName': 'CompanyName',
            'salaryInsights': 'SalaryInsights',
            'applicants': 'NoOfApplicants',
            'formattedLocation': 'CompanyLocation',
            'formattedEmploymentStatus': 'EmploymentStatus',
            'formattedExperienceLevel': 'ExperienceLevel',
            'formattedIndustries': 'Industries',
            'jobDescription': 'JobDescription',
            'inferredBenefits': 'Benefits',
            'jobFunctions': 'JobFunctions',
            'companyApplyUrl': 'CompanyApplicationUrl',
            'jobPostingUrl': 'JobPostingUrl',
            'listedAt': 'PostedDate'
        }
        df.rename(columns=column_to_rename, inplace=True)

        # Display column data types before making any changes
        print('Column data types before conversion:\n', df.dtypes)

        # Change specific columns to numeric types for consistency
        columns_to_change = ['CompanyId', 'SalaryInsights', 'NoOfApplicants']
        for col in columns_to_change:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Convert date columns to datetime format for easier analysis
        if 'PostedDate' in df.columns:
            df['PostedDate'] = pd.to_datetime(df['PostedDate'], errors='coerce')

# Convert PostedDate to string before performing any string operations
        df['PostedDate'] = df['PostedDate'].astype(str)

        # Now remove the +00:00 part from the timestamp
        df['PostedDate'] = df['PostedDate'].str.split('+').str[0]  # Remove timezone part

        # Convert PostedDate back to datetime if needed after removal of +00:00
        df['PostedDate'] = pd.to_datetime(df['PostedDate'], errors='coerce')

        # Display column data types after making conversions
        print('Column data types after conversion:\n', df.dtypes)

        # Drop columns that are unnecessary or redundant
        columns_to_drop = [
            'CompanyURL1', 'CompanyUniversalName', 
            'JobFunctions', 'CompanyApplicationUrl', 'JobDescription'
        ]
        df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)

        # Display the transformed DataFrame
        print('Transformed DataFrame:\n', df)

        # Save the transformed DataFrame to a CSV file
        df.to_csv('flattened_job_postings.csv', index=False)
        print('Data saved to "flattened_job_postings.csv".')

    else:
        # Raise an error if the expected 'response' field is missing
        raise ValueError("Unexpected JSON format or 'response' field missing.")

except Exception as e:
    # Print any error encountered during processing
    print('Error processing JSON data:', e)

####################################################################################################################


#%% Step 3: Load Data into a Database

# Import necessary libraries for database operations
import pyodbc
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

# Define the database connection string for connecting to SQL Server
DATABASE_CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ARMSTRONG;"
    "Database=linkedin_joblist;"
    "Trusted_Connection=yes;"
)

# Set up logging to track script activity
log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Add console logging for real-time feedback
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# Log the start of the script
logging.info("Script started.")

def upload_data(table, dataframe, upload_type):
    """
    Upload data to a specified table in the database.

    Parameters:
        table (str): Name of the table to upload data.
        dataframe (DataFrame): Pandas DataFrame containing data to upload.
        upload_type (str): Method of upload ('replace', 'append', etc.).

    Returns:
        None
    """
    try:
        logging.info("Attempting to connect to the database for uploading data.")
        # Create an SQLAlchemy engine for database connection
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        # Upload the DataFrame to the database table
        logging.info(f"Uploading data to table: {table}")
        dataframe.to_sql(table, engine, index=False, if_exists=upload_type, schema="dbo", chunksize=10000)
        logging.info(f"Data uploaded successfully to {table}.")
    except Exception as e:
        # Log any errors that occur during the upload process
        logging.error(f"Error uploading data: {e}")
        print(f"Error uploading data: {e}")


# Specify the table name and upload type
table_name = "new_linkedin_table"
upload_type = "append"  # Options: 'replace', 'append'

# Upload the transformed data to the database
try:
    upload_data(table_name, df, upload_type)
    logging.info("Data uploaded successfully.")
    print("Data uploaded successfully.")
except Exception as e:
    logging.error(f"Failed to upload data: {e}")
    print(f"Failed to upload data: {e}")

# Log the end of the script
logging.info("Script ended.")






# %%
