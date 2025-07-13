# Telegram Data Pipeline: From Raw Data to Analytical Insights

## Project Overview

This project implements an end-to-end data pipeline to extract, load, and transform data from public Telegram channels. The goal is to build a robust data platform capable of generating insights, particularly focused on Ethiopian medical businesses and product-related information.

The pipeline follows an ELT (Extract, Load, Transform) approach:
* **Extract:** Scrapes messages and associated media (images) from designated Telegram channels.
* **Load:** Stores the raw, unstructured data into a data lake (file system) and then into a PostgreSQL database.
* **Transform:** Utilizes dbt (data build tool) to clean, restructure, and model the raw data into a star schema suitable for analytical querying.

## Features

* **Telegram Scraper:** Fetches messages and images, handling `datetime` and `bytes` serialization for JSON storage.
* **Data Lake Storage:** Raw JSON messages and images are stored in a partitioned file system structure.
* **PostgreSQL Data Warehouse:** Raw data is loaded into PostgreSQL, which serves as the analytical database.
* **dbt Data Transformation:** Transforms raw data into clean, denormalized dimension (e.g., `dim_channels`, `dim_dates`) and fact (`fct_messages`) tables using SQL.
* **Dockerized Environment:** All services (PostgreSQL database, Python application for scraping/loading, dbt) are containerized using Docker Compose for easy setup and consistent environments.

## Project Structure

### ├── .env                  # Environment variables(API keys, DB credentials)
#### ├── docker-compose.yml    # Defines Docker services (PostgreSQL, Python app)
#### ├── README.md             # This file
#### ├── scraper.log           # Log file for the scraping process
#### ├── data_loader.log       # Log file for the data loading process
#### ├── data/                 # Data lake directory
#### │   ├── raw/
#### │       ├── telegram_messages/
#### │           ├── YYYY-MM-DD/
#### │           │   ├── channel_username/
#### │           │   │   └── channel_username_YYYY-MM-DD.json
#### │           └── images/   # Downloaded images
#### ├── my_telegram_dbt_project/ # Your dbt project directory
#### │   ├── dbt_project.yml
#### │   ├── packages.yml
#### │   ├── models/
#### │   │   ├── marts/
#### │   │   │   ├── dim_channels.sql
#### │   │   │   ├── dim_dates.sql
#### │   │   │   └── fct_messages.sql
#### │   │   ├── staging/
#### │   │   │   └── stg_telegram_messages.sql
#### │   │   └── schema.yml    # Source definitions, model descriptions, and tests
#### │   └── profiles.yml      # dbt database connection profiles (typically in ~/.dbt/)
#### └── src/                  # Source code for Python scripts
#### ├── init.py
#### ├── scraper.py        # Telegram data scraping script
#### └── load_raw_to_postgres.py # Script to load raw JSON into PostgreSQL

