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

import csv
import json
import os


from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
import pandas as pd
from confluent_kafka.serialization import StringSerializer


employee_topic_name = "bf_employee_salary"
csv_file = 'Employee_Salaries.csv'
filtered_departments = {'ECC', 'CIT', 'EMS'}
# You can adjust the batch_size_list for different testing scenarios
batch_size_list = [10, 50, 100, 200, 500]

#Can use the confluent_kafka.Producer class directly
class salaryProducer(Producer):
    #if connect without using a docker: host = localhost and port = 29092
    #if connect within a docker container, host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
     

class DataHandler:
    def __init__(self, file_path):
        self.file_path = file_path

    def get_filtered_employees(self):
        df = pd.read_csv(self.file_path, header=None)
        df.columns = ['Department', 'Division', 'Code', 'Position_Title', 'Status', 'Hire_Date', 'End_Date', 'Salary']
        # Filter departments
        df = df[df['Department'].isin(filtered_departments)]
        # Filter hire_date after 2010-01-01
        df['Hire_Date'] = pd.to_datetime(df['Hire_Date'], format='%d-%b-%Y')
        df = df[df['Hire_Date'] > pd.Timestamp('2010-01-01')]
        # Round down salary
        df['Salary'] = df['Salary'].apply(lambda x: int(float(x)))
        return df.to_dict(orient='records')

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    reader = DataHandler()
    producer = salaryProducer()
    '''
    # implement other instances as needed
    # you can let producer process line by line, and stop after all lines are processed, or you can keep the producer running.
    # finish code with your own logic and reasoning

    for line in lines:
        emp = Employee.from_csv_line(line)
        producer.produce(employee_topic_name, key=encoder(emp.emp_dept), value=encoder(emp.to_json()))
        producer.poll(1)
    '''
    