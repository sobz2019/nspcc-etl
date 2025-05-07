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



## Data Model

The ETL process loads data into a star schema with the following tables:

- **fact_donations**: Main fact table containing donation transactions
- **dim_customer**: Customer dimension with SCD2 tracking
- **dim_region**: Region Dimension
- **dim_payment_method**: Payment method Dimension
- **dim_date**: Date dimension table

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