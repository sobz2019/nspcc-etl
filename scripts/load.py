# load.py
import psycopg2
from datetime import datetime
from config.db_config import DB_CONFIG
from config.settings import SCHEMA
from utils.logger import setup_logger

logger = setup_logger('load')

# SCD2 Type Handling for Dimension tables
def insert_scd2(cursor, table, keys, record):
    """
    Insert or update a record using Slowly Changing Dimension Type 2 methodology
    
    Args:
        cursor: Database cursor
        table: Target table name
        keys: List of key columns
        record: Record data as dictionary
    """
    schema_table = f"{SCHEMA}.{table}"
    key_conditions = ' AND '.join([f'{k}=%s' for k in keys])
    key_values = [record[k] for k in keys]
    
    # Check if record exists
    cursor.execute(f"SELECT 1 FROM {schema_table} WHERE {key_conditions} AND is_active='Y'",
        key_values
    )
    logger.debug(f"SELECT 1 FROM {schema_table} WHERE {key_conditions} AND is_active='Y'", key_values)
    
    # If exists, expire the current record
    if cursor.fetchone():
        cursor.execute(f"""
            UPDATE {schema_table} SET effective_end_date=%s, is_active='N'
            WHERE {key_conditions} AND is_active='Y'
        """, [datetime.today()] + key_values)

    # Insert new record
    columns = ', '.join(record.keys())
    values = ', '.join(['%s'] * len(record))
    
    cursor.execute(f"""
        INSERT INTO {schema_table} ({columns}, effective_start_date, effective_end_date, is_active)
        VALUES ({values}, %s, NULL, 'Y')
    """, list(record.values()) + [datetime.today()])

def load_to_db(fact_rows, dim_customers, dim_payments, dim_regions):
    """
    Load transformed data into database tables
    """
    logger.info("Starting database load process")
    conn = None
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        unique_regions = {}
        for rec in dim_regions:
            unique_regions[rec['region']] = rec
        
        
        unique_payments = {}
        for rec in dim_payments:
            unique_payments[rec['payment_method']] = rec
        
        # Load dimension tables with unique records
        logger.info(f"Loading {len(unique_regions)} unique region records")
        for rec in unique_regions.values():
            insert_scd2(cursor, "dim_region", ["region"], rec)
            
        # 
        for rec in dim_customers:
            insert_scd2(cursor, "dim_customer", ["customer_id"], rec)
            
        logger.info(f"Loading {len(unique_payments)} unique payment method records")
        for rec in unique_payments.values():
            insert_scd2(cursor, "dim_payment_method", ["payment_method"], rec)

        # Load fact table
        logger.info(f"Loading {len(fact_rows)} fact records")
        loaded_count = 0
        skipped_count = 0
        
        for row in fact_rows:
            try:
                # Get surrogate keys from dimension tables
                cursor.execute(
                    f"SELECT customer_key FROM {SCHEMA}.dim_customer WHERE customer_id = %s AND is_active = TRUE", 
                    (row["customer_id"],)
                )
                customer_key = cursor.fetchone()
                
                cursor.execute(
                    f"SELECT region_key FROM {SCHEMA}.dim_region WHERE region = %s AND is_active = TRUE", 
                    (row["region"],)
                )
                region_key = cursor.fetchone()
            
                cursor.execute(
                    f"SELECT payment_method_key FROM {SCHEMA}.dim_payment_method WHERE payment_method = %s AND is_active = TRUE", 
                    (row["payment_method"],)
                )
                payment_method_key = cursor.fetchone()
            
                cursor.execute(
                    f"SELECT date_id FROM {SCHEMA}.dim_date WHERE full_date = %s", 
                    (row["payment_date"],)
                )
                date_key = cursor.fetchone()
            
                if not (customer_key and region_key and payment_method_key and date_key):
                    logger.warning(f"Skipping row {row['payment_id']} due to missing dimension keys")
                    skipped_count += 1
                    continue
            
                # Insert into fact_donations
                cursor.execute(f"""
                    INSERT INTO {SCHEMA}.fact_donations (
                        payment_id, customer_key, date_key, payment_method_key, region_key,
                        amount, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (payment_id) DO NOTHING
                """, (
                    row["payment_id"],
                    customer_key[0],
                    date_key[0],
                    payment_method_key[0],
                    region_key[0],
                    row["amount"],
                    row["status"]
                ))
                loaded_count += 1
                
            except Exception as e:
                logger.error(f"Error loading fact row {row['payment_id']}: {e}")
                skipped_count += 1

        logger.info(f"Fact table load complete. Loaded: {loaded_count}, Skipped: {skipped_count}")
        conn.commit()
        
    except Exception as e:
        logger.error(f"Database load failed: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        
    logger.info("Database load process complete")