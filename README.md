# NSPCC ETL Data Pipeline

A robust ETL (Extract, Transform, Load) solution for processing NSPCC donation data into a data warehouse.

## Overview

This project implements a modular ETL pipeline for the National Society for the Prevention of Cruelty to Children (NSPCC) to process donation data. It extracts data from JSON files, transforms it into a star schema structure, and loads it into a PostgreSQL data warehouse using Slowly Changing Dimension (SCD) Type 2 methodology.

## Features

- **Modular Architecture**: Separate extract, transform, and load components
- **Slowly Changing Dimension (SCD2)**: Tracks historical changes in dimension tables
- **Robust Logging**: Timestamped logs stored in dedicated logs directory
- **Error Handling**: Comprehensive error logging and reporting
- **Performance Tracking**: Monitors processing time and record counts

## Project Structure
```
nspcc_etl/
├── config/
│   ├── init.py
│   ├── db_config.py      # Database connection settings
│   └── settings.py       # Global settings and paths
├── data/                 # Input JSON files directory
├── logs/                 # Log files directory
├── scripts/
│   ├── init.py
│   ├── extract.py        # Data extraction module
│   ├── transform.py      # Data transformation module
│   ├── load.py           # Database loading module
│   └── main.py           # Main ETL execution script
├── sql/
│   └── creation_table_scripts.sql  # Database schema creation scripts
├── utils/
│   ├── init.py
│   └── logger.py         # Logging configuration
├── init.py
├── run_etl.py            # Entry point script
├── setup.py              # Package installation script
└── requirements.txt      # Dependencies
```


## Data Model

The ETL process loads data into a star schema with the following tables:

- **fact_donations**: Main fact table containing donation transactions
- **dim_customer**: Customer dimension with SCD2 tracking
- **dim_region**: Region Dimension
- **dim_payment_method**: Payment method Dimension
- **dim_date**: Date dimension table

 - **file_error_logging**: File Processing Log Error Tracking Table

## Installation

1. Clone the repository:

git clone https://github.com/YOUR-USERNAME/nspcc-etl.git
cd nspcc-etl

2. Install the package in development mode:

pip install -e .

3. Set up your PostgreSQL database using the scripts in the `sql` directory

## Usage

Run the ETL process:
python run_etl.py

Or run individual components:
python scripts/extract.py  # Run only the extraction phase
python scripts/transform.py  # Run only the transformation phase
python scripts/load.py  # Run only the loading phase



### Analytical Questions Addressed

1. **Unique Donors**: Count of unique individuals who made donations
2. **Donation Metrics**: Total and average donation amount calculations
3. **Donor Characteristics Analysis**: Correlation between user profiles and donation patterns
4. **Failed Payments**: Count and analysis of failed donation payments
5. **Geographic Distribution**: Proportion of donations from London vs. outside London

### Analytics Implementation

The ETL pipeline specifically:

1. Extracts donation data from JSON files
2. Identifies and flags London-based donations using the region field
3. Transforms customer profile data to enable demographic analysis
4. Loads data into a dimensional model optimized for analytical queries
5. Tracks payment status to enable failed payment analysis

Once loaded, analysts can run queries like:

```sql

-- Count unique donors
SELECT COUNT(DISTINCT customer_key) FROM fact_donations;

-- Total and average donation amounts
SELECT SUM(amount) as Total_Amount, AVG(amount) as Avg_Amount FROM fact_donations 
WHERE status = 'completed' and amount >0;

-- Donations by region (London vs non-London)
SELECT 
    CASE 
        WHEN r.is_london = TRUE THEN 'London'
        ELSE 'Non-London'
    END AS region,
    COUNT(*) AS donation_count
FROM fact_donations f
JOIN dim_region r ON f.region_key = r.region_key
WHERE f.amount > 0
GROUP BY 
    CASE 
        WHEN r.is_london = TRUE THEN 'London'
        ELSE 'Non-London'
    END;

-- Failed donations count
SELECT COUNT(*) FROM fact_donations WHERE status != 'failed';

-- Donor characteristics correlation
SELECT c.shirt_size, c.donates_to_charity, c.bikes_to_work,
       COUNT(*) as donation_count, AVG(f.amount) as avg_donation
FROM fact_donations f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.shirt_size, c.donates_to_charity, c.bikes_to_work
ORDER BY avg_donation DESC;