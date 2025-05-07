"""Database configuration for the NSPCC ETL process."""

# PostgreSQL connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'nspcc_dwh',
    'user': 'postgres',
    'password': 'admin'  # In production, use environment variables
}

# SQLAlchemy connection string
CONNECTION_STRING = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# Schema name
SCHEMA = 'nspcc'

# Directory paths
DATA_DIR = 'data'
LOG_DIR = 'logs'

# Processed files tracking
PROCESSED_FILES_TABLE = f"{SCHEMA}.processed_files"