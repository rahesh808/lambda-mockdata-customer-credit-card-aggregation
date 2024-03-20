import csv
import os
import random
import boto3
from faker import Faker
from datetime import datetime, timedelta

def generate_mock_data(num_records):
    fake = Faker()
    transactions = []
    headers = ["customer_id", "name", "debit_card_number", "debit_card_type", "bank_name", "transaction_date", "amount_spend"]
    transactions.append(headers)
    for _ in range(num_records):
        customer_id = fake.random_int(min=1000, max=9999)
        name = fake.name()
        debit_card_number = fake.credit_card_number(card_type=None)
        debit_card_type = fake.credit_card_provider(card_type=None)
        bank_name = "Axis"
        transaction_date = fake.date_this_month(before_today=True, after_today=False)
        amount_spend = round(random.uniform(10.0, 500.0), 2)
        transactions.append([customer_id, name, debit_card_number, debit_card_type, bank_name, transaction_date, amount_spend])
    return transactions

def save_to_s3(bucket_name, filename, data):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=filename, Body=data)

def lambda_handler(event, context):
    # Set up parameters
    start_date = datetime.utcnow().date() - timedelta(days=1)  # Yesterday's date
    end_date = datetime.utcnow().date() - timedelta(days=1)    # Yesterday's date
    bucket_name = 'csvcollectorrahesh'

    # Generate mock data
    transactions = generate_mock_data(10)
    csv_data = ''.join([','.join(map(str, row)) + '\n' for row in transactions])

    # Save data to CSV file and upload to S3
    filename = f"customer_debit_card_{start_date.strftime('%Y_%m_%d')}.csv"
    save_to_s3(bucket_name, filename, csv_data)

    return {
        'statusCode': 200,
        'body': f"Saved {len(transactions)} records to S3 bucket {bucket_name} with filename {filename}"
    }
