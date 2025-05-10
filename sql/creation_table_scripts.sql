-- Create schema for NSPCC data warehouse
drop schema nspcc cascade;
-- Create schema for NSPCC data warehouse
CREATE SCHEMA IF NOT EXISTS nspcc;

-- Dimension table for customers/donors with SCD Type 2
CREATE TABLE IF NOT EXISTS nspcc.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    email VARCHAR(255),
    shirt_size VARCHAR(50),
    donates_to_charity VARCHAR(50),
    bikes_to_work VARCHAR(50),
    effective_start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT true,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Dimension table for dates
CREATE TABLE IF NOT EXISTS nspcc.dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    UNIQUE(full_date)
);

-- Dimension table for payment methods with SCD Type 2
CREATE TABLE IF NOT EXISTS nspcc.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method VARCHAR(50) NOT NULL,
    payment_category VARCHAR(50),
    is_card BOOLEAN,
    effective_start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT true,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension table for regions with SCD Type 2
CREATE TABLE IF NOT EXISTS nspcc.dim_region (
    region_key SERIAL PRIMARY KEY,
    region VARCHAR(100) NOT NULL,
    is_london BOOLEAN NOT NULL,
    country VARCHAR(50) DEFAULT 'United Kingdom',
    effective_start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact table for donations
CREATE TABLE IF NOT EXISTS nspcc.fact_donations (
    donation_key SERIAL PRIMARY KEY,
    payment_id INT NOT NULL,
    customer_key INT NOT NULL REFERENCES nspcc.dim_customer(customer_key),
    date_key INT NOT NULL REFERENCES nspcc.dim_date(date_id),
    payment_method_key INT NOT NULL REFERENCES nspcc.dim_payment_method(payment_method_key),
    region_key INT NOT NULL REFERENCES nspcc.dim_region(region_key),
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    source_name VARCHAR(255) default 'file',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(payment_id)
);

-- Table to track processed files for incremental processing
CREATE TABLE IF NOT EXISTS nspcc.file_error_logging (
    file_name VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_count INT NOT NULL,
    processing_time_seconds INT,
    status VARCHAR(20) DEFAULT 'completed',
    error_message TEXT
);

-- Function for populating the Date dimension
CREATE OR REPLACE FUNCTION nspcc.populate_date_dimension(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    curr_date DATE := start_date;
BEGIN
    WHILE curr_date <= end_date LOOP
        INSERT INTO nspcc.dim_date (
            full_date,
            day_of_month,
            day,
            month,
            year,
            quarter,
            month_name,
            day_of_week,
            day_name,
            is_weekend
        )
        VALUES (
            curr_date,
            EXTRACT(DAY FROM curr_date),
            EXTRACT(DAY FROM curr_date),
            EXTRACT(MONTH FROM curr_date),
            EXTRACT(YEAR FROM curr_date),
            EXTRACT(QUARTER FROM curr_date),
            TO_CHAR(curr_date, 'Month'),
            EXTRACT(DOW FROM curr_date),
            TO_CHAR(curr_date, 'Day'),
            CASE WHEN EXTRACT(DOW FROM curr_date) IN (0, 6) THEN TRUE ELSE FALSE END
        )
        ON CONFLICT (full_date) DO NOTHING;

        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;



-- Populate date dimension with data from 2020 to 2025
SELECT nspcc.populate_date_dimension('2020-01-01', '2030-12-31');