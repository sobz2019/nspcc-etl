# Extract data from JSON files
import os
import json
import time
import psycopg2
from config.db_config import DB_CONFIG
from config.settings import DATA_DIR, SCHEMA
from utils.logger import setup_logger

# Set up logger for this module
logger = setup_logger('extract')

def load_json_files():
    """
    Load and process JSON files from the data directory
    """
    all_data = []
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    logger.info(f"Starting to process JSON files from {DATA_DIR}")
    
    for file in os.listdir(DATA_DIR):
        if not file.endswith(".json"):
            logger.debug(f"Skipping non-JSON file: {file}")
            continue

        file_path = os.path.join(DATA_DIR, file)
        logger.info(f"Processing file: {file_path}")
        start_time = time.time()
        record_count = 0
        status = 'completed'
        error_message = None

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                all_data.extend(data)
                record_count = len(data)
                logger.info(f"Successfully loaded {record_count} records from {file}")
        except Exception as e:
            status = 'failed'
            error_message = str(e)
            logger.error(f"JSON decode error in file: {file} -> {e}")

        processing_time = int(time.time() - start_time)
        logger.info(f"File {file} processed in {processing_time} seconds")

        # Insert into processed_file_log
        try:
            cursor.execute(f"""
                INSERT INTO {SCHEMA}.file_error_logging 
                (file_name, record_count, processing_time_seconds, status, error_message)
                VALUES (%s, %s, %s, %s, %s)
            """, (file, record_count, processing_time, status, error_message))
            logger.info(f"Logged file processing status for {file}")
        except Exception as e:
            logger.error(f"Failed to log file processing status: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Completed processing all JSON files. Total records: {len(all_data)}")
    return all_data