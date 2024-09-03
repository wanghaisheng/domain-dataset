import csv
import io
import psycopg2
from psycopg2 import sql, OperationalError
import logging
import time
from tqdm import tqdm
import os
import tempfile
import glob

## domcop data load to db

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Connection parameters
conn_params = {
    'host': 'shortly-strong-skylark-pdt.a1.pgedge.io',
    'user': 'admin',
    'database': 'domains',
    'password': 'xxxxxxxxxx',
    'sslmode': 'require',
    'connect_timeout': 10  # Timeout in seconds
}

def connect_with_retries(params, max_retries=3, delay=5):
    for attempt in range(max_retries):
        try:
            connection = psycopg2.connect(**params)
            return connection
        except OperationalError as e:
            logging.error(f"Connection attempt {attempt + 1} failed: {e}")
            time.sleep(delay)
    raise Exception("Failed to connect to the database after several attempts")

def insert_domains_temp_table(connection, file_path, domain_column, batch_size=10000):
    logging.debug(f"Starting insertion of domains from {file_path} into the temp table.")
    
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS temp_domains (
                domain TEXT PRIMARY KEY
            )
            """)
            
            # Process the CSV file in batches
            with open(file_path, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                if domain_column not in reader.fieldnames:
                    raise ValueError(f"Column '{domain_column}' not found in CSV file.")
                
                # Prepare to track the progress
                total_lines = sum(1 for _ in open(file_path, mode='r', newline='', encoding='utf-8'))
                file.seek(0)  # Reset file pointer to the start
                reader = csv.DictReader(file)  # Re-initialize reader

                batch = []
                logging.debug(f"Processing CSV file in batches of {batch_size} domains.")
                progress_bar = tqdm(total=total_lines, unit='lines', desc='Processing CSV')
                
                for row in reader:
                    domain = row[domain_column]
                    batch.append(domain)
                    progress_bar.update(1)
                    
                    if len(batch) >= batch_size:
                        # Insert the batch into the temp table
                        csv_data = io.StringIO()
                        writer = csv.writer(csv_data)
                        writer.writerow(['domain'])
                        writer.writerows((d,) for d in batch)
                        csv_data.seek(0)
                        
                        try:
                            with connection.cursor() as cursor:
                                cursor.copy_expert(
                                    sql="COPY temp_domains (domain) FROM STDIN WITH CSV HEADER",
                                    file=csv_data
                                )
                            connection.commit()
                            logging.debug(f"Inserted batch of {len(batch)} domains.")
                        except Exception as e:
                            logging.error(f"Error during COPY operation: {e}")
                            connection.rollback()
                            raise
                        
                        batch = []  # Reset the batch list
                
                # Insert any remaining domains that didn't fill the final batch
                if batch:
                    csv_data = io.StringIO()
                    writer = csv.writer(csv_data)
                    writer.writerow(['domain'])
                    writer.writerows((d,) for d in batch)
                    csv_data.seek(0)
                    
                    try:
                        with connection.cursor() as cursor:
                            cursor.copy_expert(
                                sql="COPY temp_domains (domain) FROM STDIN WITH CSV HEADER",
                                file=csv_data
                            )
                        connection.commit()
                        logging.debug(f"Inserted final batch of {len(batch)} domains.")
                    except Exception as e:
                        logging.error(f"Error during COPY operation: {e}")
                        connection.rollback()
                        raise
                
                progress_bar.close()
            
            # Insert domains into the permanent table with retry logic
            for attempt in range(3):
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("""
                        INSERT INTO domains (domain)
                        SELECT domain FROM temp_domains
                        ON CONFLICT (domain) DO NOTHING
                        """)
                    connection.commit()
                    logging.debug("Domain insertion into the permanent table completed.")
                    break
                except Exception as e:
                    logging.error(f"Error during INSERT operation: {e}")
                    connection.rollback()
                    if attempt < 2:
                        logging.debug(f"Retrying insert operation... Attempt {attempt + 2}")
                        time.sleep(10)
                    else:
                        raise

    except Exception as e:
        logging.error(f"Critical error: {e}")
        if not connection.closed:
            connection.rollback()
        raise

def find_csv_files(root_folder):
    return glob.glob(os.path.join(root_folder, '**', '*.csv'), recursive=True)

def query_domains(connection, output_csv):
    with connection.cursor() as cursor:
        cursor.execute("""
        SELECT domain, id FROM domains
        """)
        rows = cursor.fetchall()
        with open(output_csv, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['domain', 'id'])
            writer.writerows(rows)

def update_csv_with_ids(input_csv, domain_id_csv, updated_csv):
    domain_ids = {}
    with open(domain_id_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            domain_ids[row['domain']] = row['id']
    
    with open(input_csv, mode='r', newline='', encoding='utf-8') as file_in, \
         open(updated_csv, mode='w', newline='', encoding='utf-8') as file_out:
        reader = csv.DictReader(file_in)
        fieldnames = reader.fieldnames + ['domain_id']
        writer = csv.DictWriter(file_out, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            domain = row.get('domain')
            row['domain_id'] = domain_ids.get(domain, '')
            writer.writerow(row)

def find_domain_column(file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return reader.fieldnames

def process_historical_data(connection, root_folder):
    csv_files = find_csv_files(root_folder)
    
    with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8') as temp_combined:
        for file_path in tqdm(csv_files, desc='Combining CSV Files', unit='file'):
            with open(file_path, mode='r', newline='', encoding='utf-8') as file:
                temp_combined.write(file.read())
        
        temp_combined.close()
        
        # Determine the domain column name by examining the first CSV file
        fieldnames = find_domain_column(csv_files[0])
        domain_column = next((name for name in fieldnames if 'domain' in name.lower()), None)
        if domain_column is None:
            print("No suitable domain column found in CSV files.")
            return
        
        insert_domains_temp_table(connection, temp_combined.name, domain_column)
        
        domain_id_csv = 'domain_ids.csv'
        query_domains(connection, domain_id_csv)
        
        for file_path in tqdm(csv_files, desc='Updating CSV Files', unit='file'):
            updated_csv = f"updated_{os.path.basename(file_path)}"
            update_csv_with_ids(file_path, domain_id_csv, updated_csv)
            print(f"Updated CSV saved as: {updated_csv}")

def main():
    data_folder = './datas/domcop'

    connection = None
    try:
        connection = connect_with_retries(conn_params)
        process_historical_data(connection, data_folder)
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
