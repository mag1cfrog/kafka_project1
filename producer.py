"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""


import json
import logging
import sys

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
import polars as pl

from employee import Employee


EMPLOYEE_TOPIC = "bf_employee_salary"
CSV_FILE = 'Employee_Salaries.csv'
FILTERED_DEPARTMENTS = {'ECC', 'CIT', 'EMS'}
BATCH_SIZE_LIST = [10, 50, 100, 200, 500]
OPTIMAL_BATCH_SIZE = 200

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

#Can use the confluent_kafka.Producer class directly
class salaryProducer(Producer):
    #if connect without using a docker: host = localhost and port = 29092
    #if connect within a docker container, host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {
            'bootstrap.servers': f"{host}:{port}",
            'acks': 'all',
            # Adjust batch.size (default is 16384 bytes)
            'batch.size': 32768,  # 32 KB
            # Adjust linger.ms (default is 0)
            'linger.ms': 100,  # Wait up to 100 ms for more messages
            # Enable compression
            'compression.type': 'gzip',
            'security.protocol': 'PLAINTEXT' 
        }
        super().__init__(producerConfig)
     

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. Triggered by poll() or flush(). """
    if err is not None:
        logging.error(f"Delivery failed for Message: {msg.value()} - Error: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}] - Offset: {msg.offset()}")
    
    
def load_and_filter_employees(file_path):
    """
    Loads the CSV file using Polars' lazy API and filters employee records based on department and hire date.
    Utilizes lazy execution to handle large files efficiently.
    """
    try:
        # Define schema for better performance
        dtypes = {
            'Department': pl.Utf8,
            'Department-Division': pl.Utf8,
            'PCN': pl.Utf8,
            'Position Title': pl.Utf8,
            'FLSA Status': pl.Utf8,
            'Initial Hire Date': pl.Utf8,
            'Date in Title': pl.Utf8,
            'Salary': pl.Float64
        }

        # Create a LazyFrame
        lf = (
            pl.scan_csv(file_path, has_header=True, schema_overrides=dtypes)
            .rename({
                'Department-Division': 'Division',
                'Position Title': 'Position_Title',
                'FLSA Status': 'Status',
                'Initial Hire Date': 'Hire_Date',
                'Date in Title': 'End_Date'
            })
            # Filter departments
            .filter(pl.col('Department').is_in(FILTERED_DEPARTMENTS))
            # Convert 'Hire_Date' to datetime, coercing errors to null
            .with_columns([
                pl.col('Hire_Date').str.strptime(pl.Date, format='%d-%b-%Y', strict=False).alias('Hire_Date')
            ])
            # Filter hire_date after 2010-01-01
            .filter(pl.col('Hire_Date') > pl.date(2010, 1, 1))
            # Round down salary and handle nulls
            .with_columns([
                pl.when(pl.col('Salary').is_not_null())
                .then(pl.col('Salary').cast(pl.Int64))
                .otherwise(0)
                .alias('Salary')
            ])
            # Convert 'Hire_Date' to string for JSON serialization
            .with_columns([
                pl.col('Hire_Date').dt.strftime('%Y-%m-%d').alias('Hire_Date')
            ])
            # Drop rows with invalid Hire_Date
            .drop_nulls(subset=['Hire_Date'])
        )

        # Collect the LazyFrame into a DataFrame
        df = lf.collect()
        # Convert DataFrame to list of dictionaries
        return df.to_dicts()
    except Exception as e:
        logging.error(f"Error processing CSV file with Polars: {e}")
        sys.exit(1)


def calculate_message_size(batch, encoder):
    """
    Serializes the batch and returns its size in bytes.
    """
    serialized_batch = encoder(json.dumps(batch))
    return len(serialized_batch)


def send_batch(producer, batch, encoder):
    for emp_data in batch:
        emp = Employee(
            emp_dept=emp_data['Department'],
            emp_division=emp_data['Division'],
            emp_position=emp_data['Position_Title'],
            emp_hire_date=emp_data['Hire_Date'],
            emp_salary=emp_data['Salary']
        )
        producer.produce(
            EMPLOYEE_TOPIC,
            key=encoder(emp.emp_dept),
            value=encoder(emp.to_json()),
            callback=delivery_report
        )
    producer.poll(0)
    total_size = calculate_message_size(batch, encoder)
    logging.info(f"Sent {len(batch)} messages with total size: {total_size / 1024:.2f} KB.")


def test_batch_sizes(producer, employees, encoder):
    """
    Iterates through different batch sizes and prints the corresponding message sizes.
    """
    for batch_size in BATCH_SIZE_LIST:
        total_size = 0
        num_batches = 0
        batch = []
        for idx, emp_data in enumerate(employees, 1):
            batch.append(emp_data)
            if idx % batch_size == 0:
                message_size = calculate_message_size(batch, encoder)
                total_size += message_size
                num_batches += 1
                batch = []
        # Handle remaining records
        if batch:
            message_size = calculate_message_size(batch, encoder)
            total_size += message_size
            num_batches += 1
        average_size = total_size / num_batches if num_batches else 0
        logging.info(f"Batch Size: {batch_size} | Number of Batches: {num_batches} | Average Message Size: {average_size / 1024:.2f} KB")



if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    producer = salaryProducer()
    employees = load_and_filter_employees(CSV_FILE)
    
    # print("Testing different batch sizes for message sizes:")
    # test_batch_sizes(producer, employees, encoder)
    
    batch = []
    for idx, emp_data in enumerate(employees, 1):
        batch.append(emp_data)
        if idx % OPTIMAL_BATCH_SIZE == 0:
            send_batch(producer, batch, encoder)
            batch = []
    
    # Send remaining records
    if batch:
        send_batch(producer, batch, encoder)
    
    producer.flush()

    logging.info("All messages have been sent.")