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
import pandas as pd

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
    Loads the CSV file and filters employee records based on department and hire date.
    """
    try:
        df = pd.read_csv(file_path, header=None)
        df.columns = [
            'Department', 'Division', 'Code', 'Position_Title',
            'Status', 'Hire_Date', 'End_Date', 'Salary'
        ]
        # Filter departments
        df = df[df['Department'].isin(FILTERED_DEPARTMENTS)]
        # Convert 'Hire_Date' to datetime
        df['Hire_Date'] = pd.to_datetime(df['Hire_Date'], format='%d-%b-%Y', errors='coerce')
        # Filter hire_date after 2010-01-01
        df = df[df['Hire_Date'] > pd.Timestamp('2010-01-01')]
        # Round down salary
        df['Salary'] = df['Salary'].apply(lambda x: int(float(x)) if pd.notnull(x) else 0)
        # Convert 'Hire_Date' to string for JSON serialization
        df['Hire_Date'] = df['Hire_Date'].dt.strftime('%Y-%m-%d')
        # Drop rows with invalid Hire_Date
        df = df.dropna(subset=['Hire_Date'])
        return df.to_dict(orient='records')
    except Exception as e:
        logging.error(f"Error processing CSV file: {e}")
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