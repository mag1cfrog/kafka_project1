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
import sys
import logging

import psycopg2
from confluent_kafka import Consumer, KafkaError


from employee import Employee
from producer import employee_topic_name #you do not want to hard copy it

class SalaryConsumer(Consumer):
    #if connect without using a docker: host = localhost and port = 29092
    #if connect within a docker container, host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = 'salary_group'):
        self.conf = {
            'bootstrap.servers': f'{host}:{port}',
            'group.id': group_id,
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest',
            'security.protocol': 'PLAINTEXT' 
        }
        super().__init__(self.conf)
        
        #self.consumer = Consumer(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        #implement your message processing logic here. Not necessary to follow the template. 
        try:
            self.subscribe(topics)
            while self.keep_runnning:
                msg = self.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logging.error(f"Consumer error: {msg.error()}")
                    continue
                processing_func(msg)
        except KeyboardInterrupt:
            logging.info("Aborted by user\n")
        finally:
            # Close down consumer to commit final offsets.
            self.close()

#or can put all functions in a separte file and import as a module
class ConsumingMethods:
    @staticmethod
    def add_salary(msg):
        e = Employee(**(json.loads(msg.value())))
        try:
            conn = psycopg2.connect(
                #use localhost if not run in Docker
                host="localhost",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            # Insert into department_employee
            cur.execute("""
                INSERT INTO department_employee (department, department_division, position_title, hire_date, salary)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (department, department_division, position_title, hire_date) DO NOTHING;
            """, (e.emp_dept, e.emp_division, e.emp_position, e.emp_hire_date, e.emp_salary))
            # Update department_employee_salary
            cur.execute("""
                INSERT INTO department_employee_salary (department, total_salary)
                VALUES (%s, %s)
                ON CONFLICT (department) DO UPDATE
                SET total_salary = department_employee_salary.total_salary + EXCLUDED.total_salary;
            """, (e.emp_dept, e.emp_salary))
            logging.info(f"Added salary for {e.emp_dept} department")
            cur.close()
            conn.close()
        except Exception as err:
            logging.error(f"Database error: {err}")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == '__main__':
    consumer = SalaryConsumer()
    consumer.consume([employee_topic_name],ConsumingMethods.add_salary) 