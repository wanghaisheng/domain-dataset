import csv
import os
import psycopg2
from psycopg2.extras import DictCursor, execute_values
import ast
from tqdm import tqdm

def check_and_create_table(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS domain_sources (
                id SERIAL PRIMARY KEY,
                domain_id TEXT UNIQUE,
                index_date TEXT,
                about_the_source TEXT[],
                in_their_own_words TEXT,
                update_date TIMESTAMP DEFAULT now()
            )
        """)
        connection.commit()
        print("Table 'domain_sources' checked and created if not exists.")

def import_domain_source(file_path: str):
    # PostgreSQL connection settings
    conn_params = {
        'host': 'shortly-strong-skylark-pdt.a1.pgedge.io',
        'user': 'admin',
        'database': 'domains',
        'password': 'U6Avx5p2yNo312gXH51waq78',
        'sslmode': 'require'
    }

    # Create PostgreSQL connection
    connection = psycopg2.connect(**conn_params)
    cursor = connection.cursor(cursor_factory=DictCursor)

    # Check and create table if not exists
    check_and_create_table(connection)

    batch_size = 1000
    domain_batch = []
    domain_source_batch = []

    try:
        with open(file_path, 'r', newline='', encoding='utf-8', errors='replace') as csvfile:
            reader = csv.DictReader(csvfile)
            total_rows = sum(1 for row in reader)
            csvfile.seek(0)  # Reset file pointer to beginning
            reader = csv.DictReader(csvfile)  # Reinitialize reader

            with tqdm(total=total_rows, desc="Processing CSV") as pbar:
                for row in reader:
                    try:
                        domain_batch.append((row['domain'],))
                        domain_source_batch.append(row)

                        if len(domain_batch) >= batch_size:
                            process_batch(cursor, domain_batch, domain_source_batch)
                            domain_batch = []
                            domain_source_batch = []

                        pbar.update(1)

                    except Exception as e:
                        connection.rollback()
                        print(f"Error processing row {row['domain']}: {str(e)}")

                # Process any remaining rows
                if domain_batch:
                    process_batch(cursor, domain_batch, domain_source_batch)

    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
    finally:
        cursor.close()
        connection.close()

    print('Import completed')

def process_batch(cursor, domain_batch, domain_source_batch):
    try:
        # Upsert domains
        execute_values(cursor,
            "INSERT INTO domains (domain) VALUES %s ON CONFLICT (domain) DO NOTHING",
            domain_batch
        )

        # Get domain IDs
        domains = [row[0] for row in domain_batch]
        cursor.execute(
            "SELECT id, domain FROM domains WHERE domain = ANY(%s)",
            (domains,)
        )
        domain_id_map = {row['domain']: row['id'] for row in cursor.fetchall()}

        # Prepare domain source data
        domain_source_data = []
        for row in domain_source_batch:
            domain_id = domain_id_map[row['domain']]
            index_date = row['indexdate']
            about_the_source = row.get('Aboutthesource', '')
            if about_the_source:
                try:
                    about_the_source = ast.literal_eval(about_the_source)
                except:
                    about_the_source = [about_the_source]
            else:
                about_the_source = []
            in_their_own_words = row['Intheirownwords']

            domain_source_data.append((
                domain_id,
                index_date,
                about_the_source,
                in_their_own_words
            ))

        # Upsert domain sources
        execute_values(cursor,
            """
            INSERT INTO domain_sources (domain_id, index_date, about_the_source, in_their_own_words)
            VALUES %s
            ON CONFLICT (domain_id) DO UPDATE SET
                index_date = COALESCE(EXCLUDED.index_date, domain_sources.index_date),
                about_the_source = COALESCE(EXCLUDED.about_the_source, domain_sources.about_the_source),
                in_their_own_words = COALESCE(EXCLUDED.in_their_own_words, domain_sources.in_their_own_words)
            """,
            domain_source_data
        )

        cursor.connection.commit()
        print(f"Processed batch of {len(domain_batch)} domains")

    except Exception as e:
        cursor.connection.rollback()
        print(f"Error processing batch: {str(e)}")

# Usage
if __name__ == "__main__":
    csv_file_path = './datas/top-domains-1m-in-unique-2.csv'
    import_domain_source(csv_file_path)
