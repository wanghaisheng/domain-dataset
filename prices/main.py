import pandas as pd
import time
import os
import datetime
import csv

# Configuration
input_file = 'input_urls.csv'  # Path to the input CSV file containing URLs
log_file = 'processed_urls.log'  # Path to the log file where processed URLs will be stored
batch_size = 10000  # Number of URLs to process in each batch
retry_limit = 3  # Maximum number of retries per URL

def read_csv(input_file):
    """Reads the input CSV file and returns the data."""
    # Assuming no header, col1: no, col2: url
    return pd.read_csv(input_file, header=None, names=['no', 'url'])

def get_last_processed(log_file):
    """Reads the log file and returns the last processed URL number."""
    if os.path.exists(log_file):
        with open(log_file, 'r') as log:
            lines = log.readlines()
            if lines:
                return int(lines[-1].strip())  # Get the last processed URL number
    return 0

def log_processed(log_file, processed_no):
    """Logs the processed URL number."""
    with open(log_file, 'a') as log:
        log.write(f"{processed_no}\n")

def process_url(url, retry=0):
    """Process a URL with retries if it fails."""
    try:
        # Placeholder for URL processing logic (e.g., web scraping, API call)
        print(f"Processing URL: {url}")
        # Simulating URL processing time
        time.sleep(0.1)
        return f"Processed {url}"
    except Exception as e:
        if retry < retry_limit:
            print(f"Retrying URL {url} due to error: {e}")
            return process_url(url, retry + 1)
        else:
            print(f"Failed to process URL {url} after {retry_limit} retries")
            return None

def process_batch(data, start_idx, batch_size):
    """Process a batch of URLs from the given index."""
    end_idx = start_idx + batch_size
    batch = data.iloc[start_idx:end_idx]
    
    results = []
    for index, row in batch.iterrows():
        result = process_url(row['url'])
        if result:
            results.append({'no': row['no'], 'result': result})

    return results, batch

def save_results_to_csv(results, start_no, end_no):
    """Save results to a CSV file with a timestamped filename."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"processed_urls_{start_no}-{end_no}_{timestamp}.csv"
    
    if results:
        df = pd.DataFrame(results)
        df.to_csv(output_file, index=False)
        print(f"Results saved to {output_file}")

def main():
    # Load the CSV data
    data = read_csv(input_file)

    # Get the last processed URL number
    last_processed = get_last_processed(log_file)
    print(f"Resuming from URL number {last_processed}")

    # Start processing in batches
    start_idx = last_processed
    while start_idx < len(data):
        # Process the next batch
        results, batch = process_batch(data, start_idx, batch_size)

        # Log the processed URLs
        for index, row in batch.iterrows():
            log_processed(log_file, row['no'])

        # Save results to a CSV file
        save_results_to_csv(results, batch['no'].iloc[0], batch['no'].iloc[-1])

        # Update the start index for the next batch
        start_idx += batch_size
        print(f"Processed batch {batch['no'].iloc[0]} - {batch['no'].iloc[-1]}")

if __name__ == "__main__":
    main()
