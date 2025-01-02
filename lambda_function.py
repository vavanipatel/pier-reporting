import psycopg2
import pandas as pd
import boto3
from io import BytesIO, StringIO
import uuid
from datetime import datetime
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
import time
import re
import socket

# Function to check internet connectivity
def check_internet_connectivity():
    try:
        # Connect to the host -- tells us if the host is actually reachable
        host = socket.gethostbyname("www.google.com")
        s = socket.create_connection((host, 80), 2)
        s.close()
        return True
    except Exception as e:
        print(f"Internet connectivity check failed: {e}")
        return False

# Initialize S3 client
s3_client = boto3.client('s3', region_name='us-west-1')

DB_HOST = "pier.cf80kgsekpsn.us-west-1.rds.amazonaws.com"
DB_NAME = "pier_reporting"
DB_USER = "pier_rw"
DB_PWD = "Hl5BPtkkqVI3xHK"
DB_PORT = "5432"

# Table schema
TRANSACTION_TABLE_NAME = "transaction"
TRANSACTION_CREATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS transaction (
    transaction_id INT PRIMARY KEY,
    asset_id INT,
    transaction_date DATE,
    transaction_type VARCHAR,
    transaction_principal FLOAT,
    transaction_interest FLOAT,
    transaction_fees INT,
    date_received DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (asset_id) REFERENCES ASSET (asset_id) ON DELETE CASCADE
);
"""

FUND_TABLE_NAME = "fund"
FUND_CREATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS fund (
    fund_id INT PRIMARY KEY,
    fund_name VARCHAR,
    fund_abv VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

ASSET_TABLE_NAME = "asset"
ASSET_CREATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS asset (
    asset_id INT PRIMARY KEY,
    investment_id VARCHAR,
    account_id VARCHAR,
    asset_type VARCHAR,
    eval_date VARCHAR,
    servicer_id VARCHAR,
    origination_date VARCHAR,
    pier_purchase_date VARCHAR,
    grade VARCHAR,
    term FLOAT,
    term_type VARCHAR,
    currency VARCHAR,
    original_loan_amt VARCHAR,
    credit_score FLOAT,
    credit_score_type VARCHAR,
    genre VARCHAR,
    fund_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (fund_id) REFERENCES fund (fund_id) ON DELETE CASCADE
);
"""

def create_fund_table(connection):
    with connection.cursor() as cursor:
        cursor.execute(FUND_CREATE_SCHEMA)
        connection.commit()

def insert_fund_data(connection, fund_data):
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE fund")
        for _, row in fund_data.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO fund (fund_id, fund_name, fund_abv) 
                VALUES (%s, %s, %s);
                ON CONFLICT (fund_id) DO NOTHING;
            """).format(table=sql.Identifier(FUND_TABLE_NAME))
            cursor.execute(insert_query, row.values.tolist())
        connection.commit()

def load_fund_data_to_database(fund_data):
    try:
        # Database connection
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PWD,
            port= DB_PORT
        )
        # cursor = conn.cursor()

        # # Rewrite fund data
        # cursor.execute("TRUNCATE TABLE fund")
        # for _, row in fund_data.iterrows():
        #     cursor.execute("""
        #         INSERT INTO fund (fund_id, fund_name, fund_abv) 
        #         VALUES (%s, %s, %s);
        #     """, (row['fund_id'], row['fund_name'], row['fund_abv']))

        # # Commit the transaction
        # conn.commit()
        # print("Data loaded successfully in fund table.")
        print("Connected to the database.")
        
        # Create the table if it doesn't exist
        create_fund_table(conn)
        print(f"Table '{FUND_TABLE_NAME}' ensured in the database.")
        
        # Insert data
        insert_fund_data(conn, fund_data)
        print("Data inserted successfully in fund table.")

    except Exception as e:
        print(f"Error loading data: {e}")
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()

def create_asset_table(connection):
    with connection.cursor() as cursor:
        cursor.execute(ASSET_CREATE_SCHEMA)
        connection.commit()

def insert_asset_data(connection, asset_data):
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE asset")
        for _, row in asset_data.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO asset (asset_id, investment_id, account_id, asset_type, eval_date, servicer_id, origination_date, pier_purchase_date, grade, term, term_type, currency, original_loan_amt, credit_score, credit_score_type, genre, fund_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                ON CONFLICT (fund_id) DO NOTHING;
            """).format(table=sql.Identifier(ASSET_TABLE_NAME))
            cursor.execute(insert_query, row.values.tolist())
        connection.commit()


def load_asset_data_to_database(asset_data):
    try:
        # Database connection
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PWD,
            port= DB_PORT
        )
        # cursor = conn.cursor()

        # # Rewrite asset data
        # cursor.execute("TRUNCATE TABLE asset")
        # for _, row in asset_data.iterrows():
        #     cursor.execute("""
        #         INSERT INTO asset (asset_id, investment_id, account_id, asset_type, eval_date, servicer_id, origination_date, pier_purchase_date, grade, term, term_type, currency, original_loan_amt, credit_score, credit_score_type, genre, fund_id)
        #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        #     """, (row['asset_id'], row['investment_id'], row['account_id'], row['asset_type'], row['eval_date'], row['servicer_id'], row['origination_date'], row['pier_purchase_date'], row['grade'], row['term'], row['term_type'], row['currency'], row['original_loan_amt'], row['credit_score'], row['credit_score_type'], row['genre'], row['fund_id']))

        # # Commit the transaction
        # conn.commit()
        # print("Data loaded successfully in asset table.")
        print("Connected to the database.")
        
        # Create the table if it doesn't exist
        create_asset_table(conn)
        print(f"Table '{ASSET_TABLE_NAME}' ensured in the database.")
        
        # Insert data
        insert_asset_data(conn, asset_data)
        print("Data inserted successfully in asset table.")

    except Exception as e:
        print(f"Error loading data: {e}")
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()

def create_transaction_table(connection):
    with connection.cursor() as cursor:
        cursor.execute(TRANSACTION_CREATE_SCHEMA)
        connection.commit()

def insert_transaction_data(connection, transaction_data):
    with connection.cursor() as cursor:
        for _, row in transaction_data.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO transaction (transaction_id, asset_id, transaction_date, transaction_type, transaction_principal, transaction_interest, transaction_fees, date_received) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING;
            """).format(table=sql.Identifier(TRANSACTION_TABLE_NAME))
            cursor.execute(insert_query, row.values.tolist())
        connection.commit()

def load_transaction_data_to_database(transaction_data):
    try:
        # Database connection
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PWD,
            port= DB_PORT
        )
        # cursor = conn.cursor()

        # # Load and append transaction data
        # for _, row in transaction_data.iterrows():
        #     cursor.execute("""
        #         INSERT INTO transaction (transaction_id, asset_id, transaction_date, transaction_type, transaction_principal, transaction_interest, transaction_fees, date_received) 
        #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        #         ON CONFLICT (transaction_id) DO NOTHING;
        #     """, (row['transaction_id'], row['asset_id'], row['transaction_date'], row['transaction_type'], row['transaction_principal'], row['transaction_interest'], row['transaction_fees'], row['date_received']))

        print("Connected to the database.")
        
        # Create the table if it doesn't exist
        create_transaction_table(conn)
        print(f"Table '{TRANSACTION_TABLE_NAME}' ensured in the database.")
        
        # Insert data
        insert_transaction_data(conn, transaction_data)
        print("Data inserted successfully in transaction table.")

    except Exception as e:
        print(f"Error loading data: {e}")
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()

# Function to read Excel file from S3
def read_csv_from_s3(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_content = response['Body'].read()
    return pd.read_csv(BytesIO(file_content))

# Function to save DataFrame to CSV in S3
def save_csv_to_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

# Lambda handler
def lambda_handler(event, context):
    print('Event payload', event)

     # Check internet connectivity
    if not check_internet_connectivity():
        print("No internet connection")
        return {
            'statusCode': 500,
            'body': 'No internet connection'
        }

    # Extract bucket name and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    encoded_key = event['Records'][0]['s3']['object']['key']

    input_key = unquote_plus(encoded_key)

    print("input key", input_key)

    # Ensure we're only processing files in the 'input' folder
    if not input_key.startswith("RawInputFiles"):
        print(f"Skipped: {input_key} is not in the 'RawInputFiles' folder.")
        return

    # Define S3 bucket and file path
    bucket_name = 'pier-reporting'  # replace with your bucket name
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    file_name = input_key.split("/")[-1]

    print("file name", file_name)

    # Convert the file name to lowercase for case-insensitive matching
    file_name_lower = file_name.lower()
    
    file_type = ''
    # Check for specific keywords
    if "transaction" in file_name_lower:
        file_type = 'transaction'
    elif "fund" in file_name_lower:
        file_type = 'fund'
    elif "asset" in file_name_lower:
        file_type = 'asset'
    else:
        print(f"Skipped: {file_name_lower} is not valid file name.")
        return

    print("file_type: ", file_type)

    # Define paths
    datetime_folder = f"{current_time}/"
    to_be_processed_folder = f"ToBeProcessed/{datetime_folder}"
    processed_folder = f"Processed/{datetime_folder}"
    processed_success_folder = f"{processed_folder}Success/"
    processed_failed_folder = f"{processed_folder}Failed/"
    output_folder = "Output/"
    archived_folder = f"{output_folder}Archived/"

    try:
        # Copy the file to the "ToBeProcesses" folder
        to_be_processed_key = f"{to_be_processed_folder}{file_name}"
        print("copying to_be_processed_key: ", to_be_processed_key)
        print("copying from input key: ", input_key)
        print("bucket name", bucket_name)
        # try:
        #     s3_client.copy_object(
        #         Bucket=bucket_name,
        #         CopySource={'Bucket': "pier-reporting", 'Key': "RawInputFiles/fund.csv"},
        #         Key=to_be_processed_key
        #     )
        #     print(f"Copied file to {to_be_processed_key}")
        # except Exception as e:
        #     print("error copying the file in tobeprocessed folder", e)
        #     return

        transaction_file_name = "transaction.csv"
        fund_file_name = "fund.csv"
        asset_file_name = "asset.csv"

        transaction_key = f"{output_folder}{transaction_file_name}"
        fund_key = f"{output_folder}{fund_file_name}"
        asset_key = f"{output_folder}{asset_file_name}"

        # Read CSV file from S3
        csv_data = read_csv_from_s3(bucket=bucket_name, key=input_key)
        print("excel read done got the csv_data")

        object_key = ""
        archived_file_key = ""
        if file_type == "transaction":
            object_key = transaction_key
            archived_file_key = f"{archived_folder}{current_time}_{transaction_file_name}"
        elif file_type == "fund":
            object_key = fund_key
            archived_file_key = f"{archived_folder}{current_time}_{fund_file_name}"
        elif file_type == "asset":
            object_key = asset_key
            archived_file_key = f"{archived_folder}{current_time}_{asset_file_name}"

        print("object_key: ", object_key)
        print("archived_file_key: ", archived_file_key)
        try:
            # Attempt to copy the object
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': object_key},
                Key=archived_file_key
            )
        except ClientError as e:
            # Handle specific error if the source object does not exist
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"Error: The object '{object_key}' does not exist in bucket '{bucket_name}'.")
            else:
                # Handle other exceptions
                print(f"Unexpected error: {e}")
                raise RuntimeError("Error Copy Object") from e

        # Save the reordered DataFrame to a new CSV file in S3
        save_csv_to_s3(csv_data, bucket_name, object_key)
        print("saved to s3: ", object_key)

        if file_type == "transaction":
            load_transaction_data_to_database(csv_data)
        elif file_type == "fund":
            load_fund_data_to_database(csv_data)
        elif file_type == "asset":
            load_asset_data_to_database(csv_data)

        # Copy the file to the "ToBeProcesses" folder
        processed_success_key = f"{processed_success_folder}{file_name}"
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': input_key},
            Key=processed_success_key
        )
        print(f"Copied file to {processed_success_key}")

        return {
            'statusCode': 200,
            'body': 'CSV OUTPUT files are created and uploaded successfully to S3 and Database!'
        }
    except Exception as e:
        error_message = f"Error processing file '{input_key}' and uploaded to '{processed_failed_folder}': {str(e)}"
        print(error_message)

        # Copy the file to the "Processed/Failed" folder
        processed_failed_key = f"{processed_failed_folder}{file_name}"
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': input_key},
            Key=processed_failed_key
        )
        print(f"Copied file to {processed_failed_key}")
        return {
            'statusCode': 500
        }       
