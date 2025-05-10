# main.py
import os
import sys

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from datetime import datetime
from utils.logger import setup_logger
from scripts.extract import load_json_files
from scripts.transform import flatten_data
from scripts.load import load_to_db

def main():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    logger = setup_logger(f'etl_main_{timestamp}')
    
    logger.info("=" * 50)
    logger.info("Starting NSPCC ETL process")
    logger.info("=" * 50)
    
    try:
        # Extract step
        logger.info("Starting extraction phase")
        raw_data = load_json_files()
        logger.info(f"Extraction complete: {len(raw_data)} customer records loaded")
        
        # Transform step
        logger.info("Starting transformation phase")
        fact_rows, dim_customers, dim_payments, dim_regions = flatten_data(raw_data)
        logger.info("Transformation complete")
        
        # Load step
        logger.info("Starting load phase")
        load_to_db(fact_rows, dim_customers, dim_payments, dim_regions)
        logger.info("Load phase complete")
        
        logger.info("=" * 50)
        logger.info("ETL process completed successfully")
        logger.info("=" * 50)
        return 0
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        logger.info("=" * 50)
        logger.info("ETL process failed")
        logger.info("=" * 50)
        return 1

if __name__ == "__main__":
    sys.exit(main())