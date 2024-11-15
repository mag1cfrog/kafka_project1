import logging
from multiprocessing import Process
import subprocess
import sys
import socket
import time

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
        logging.error(f"Error connecting to PostgreSQL: {e}")


def wait_for_kafka(host, port, timeout=60):
    logging.info(f"Waiting for Kafka to be available at {host}:{port}...")
    start_time = time.time()
    while True:
        try:
            # Attempt to create a socket connection
            with socket.create_connection((host, port), timeout=5):
                logging.info("Kafka is ready to accept connections.")
                break
        except (socket.timeout, ConnectionRefusedError):
            if time.time() - start_time > timeout:
                logging.error("Timeout reached. Kafka is not available.")
                sys.exit(1)
            logging.info("Kafka not ready yet. Retrying in 2 seconds...")
            time.sleep(2)


def run_producer():
    try:
        subprocess.run([sys.executable, 'producer.py'], check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Producer encountered an error: {e}")
    except KeyboardInterrupt:
        logging.info("Producer process interrupted by user.")


def run_consumer():
    try:
        subprocess.run([sys.executable, 'consumer.py'], check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Consumer encountered an error: {e}")
    except KeyboardInterrupt:
        logging.info("Consumer process interrupted by user.")



if __name__ == '__main__':
    try:
        # Set up logging with detailed record of which script each log message comes from
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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
        logging.info("Keyboard interrupt received.")
    finally:
        logging.info("Stopping Docker containers...")
        stop_docker_compose()
        logging.info("Docker containers stopped.")