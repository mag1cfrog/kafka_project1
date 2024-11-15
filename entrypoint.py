import subprocess
import time
from multiprocessing import Process
import sys
import socket

import psycopg2


def start_docker_compose():
    subprocess.run(['docker-compose', 'up', '-d'])


def stop_docker_compose():
    subprocess.run(['docker-compose', 'down'])


def check_postgres_tables():
    # Wait for PostgreSQL to be up
    time.sleep(10)
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgres",
            port="5432"
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Drop existing tables
        cur.execute("DROP TABLE IF EXISTS department_employee_salary;")
        cur.execute("DROP TABLE IF EXISTS department_employee;")

        # Create department_employee table if not exists
        cur.execute("""
        CREATE TABLE IF NOT EXISTS department_employee(
            department VARCHAR(100),
            department_division VARCHAR(150),
            position_title VARCHAR(150),
            hire_date DATE,
            salary DECIMAL,
            UNIQUE (department, department_division, position_title, hire_date)
        );
        """)

        # Create department_employee_salary table if not exists
        cur.execute("""
        CREATE TABLE IF NOT EXISTS department_employee_salary(
            department VARCHAR(100) PRIMARY KEY,
            total_salary BIGINT
        );
        """)
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")


def wait_for_kafka(host, port, timeout=60):
    print(f"Waiting for Kafka to be available at {host}:{port}...")
    start_time = time.time()
    while True:
        try:
            # Attempt to create a socket connection
            with socket.create_connection((host, port), timeout=5):
                print("Kafka is ready to accept connections.")
                break
        except (socket.timeout, ConnectionRefusedError):
            if time.time() - start_time > timeout:
                print("Timeout reached. Kafka is not available.")
                sys.exit(1)
            print("Kafka not ready yet. Retrying in 2 seconds...")
            time.sleep(2)


def run_producer():
    subprocess.run([sys.executable, 'producer.py'])


def run_consumer():
    subprocess.run([sys.executable, 'consumer.py'])


if __name__ == '__main__':
    try:
        start_docker_compose()
        check_postgres_tables()

        # Wait until Kafka is ready
        wait_for_kafka('localhost', 29092, timeout=120)

        # Start producer and consumer processes
        p1 = Process(target=run_producer)
        p2 = Process(target=run_consumer)

        p1.start()
        p2.start()

        p1.join()
        p2.join()

    except KeyboardInterrupt:
        print("Keyboard interrupt received.")
    finally:
        print("Stopping Docker containers...")
        stop_docker_compose()