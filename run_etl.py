#!/usr/bin/env python
"""
Wrapper script to run the NSPCC ETL process from the root directory.
This avoids Python import issues.
"""
import os
import sys
import argparse

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.main import run_etl
from config.db_config import DATA_DIR

def main():
    """Parse arguments and run ETL process."""
    parser = argparse.ArgumentParser(description='NSPCC ETL Process')
    parser.add_argument('--data-dir', default=DATA_DIR, help='Directory containing JSON files')
    parser.add_argument('--process-all', action='store_true', help='Process all files, including already processed ones')
    args = parser.parse_args()
    
    # Run the ETL process
    success = run_etl(args.data_dir, args.process_all)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())