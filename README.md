Coin & Crypto ETL Project
Project Overview

This project automates the ETL (Extract, Transform, Load) process for cryptocurrency market data using the CoinGecko API, Python, Prefect, and PostgreSQL. The cleaned and structured data is then connected to Power BI to create interactive dashboards for analysis and visualization.

Key objectives:

Automate fetching of cryptocurrency market data

Transform and clean data for analysis

Load data into a PostgreSQL database

Enable dashboard visualization via Power BI

Features

Data Extraction: Pulls live cryptocurrency data (Bitcoin, Ethereum, Ripple, Cardano) from CoinGecko API.

Data Transformation: Converts prices and market cap to numeric types, removes duplicates.

Data Loading: Stores the processed data in PostgreSQL (coin_crypto_data table).

Retry Mechanism: Tasks automatically retry in case of failure.

Power BI Integration: Connects to the database to create interactive dashboards displaying live cryptocurrency metrics.

Technologies Used

Python Libraries: requests, pandas, sqlalchemy, prefect, logging, psycopg2

Database: PostgreSQL

Workflow Orchestration: Prefect

Data Visualization: Power BI
