import requests
import pandas as pd
from datetime import datetime, timezone
import logging
from sqlalchemy import create_engine
from prefect import flow, task
import psycopg2
from prefect.blocks.system import Secret

logging.basicConfig(
filename=r"C:\Users\khany\OneDrive\Desktop\Stuff\Richfield studies\DE_projects\PipeLines\Coin_Crypto\coin_crypto_log.log",
level=logging.INFO,
format="%(asctime)s-%(levelname)s-%(message)s",
filemode='a'
)

@task(retries = 4, retry_delay_seconds = 10)
def extract():
    logging.info("Starting data extraction from CoinGecko API")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": "bitcoin,ethereum,ripple,cardano",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1
    }
    response = requests.get(url, params=params)
    data = response.json()

    table_data = []
    extracted_at = datetime.now(timezone.utc)

    for coin in data:
        table_data.append({
            "Coin": coin["name"],
            "Price": coin["current_price"],
            "Market Cap": coin["market_cap"],
            "extracted_at": extracted_at
        })
    logging.info("Data extraction completed successfully")
    return table_data

@task(retries = 4, retry_delay_seconds = 10)
def transform(table_data):
    logging.info("Transforming data into DataFrame")
    df = pd.DataFrame(table_data)
    df["Price"] = pd.to_numeric(df["Price"])
    df["Market Cap"] = pd.to_numeric(df["Market Cap"])
    df["extracted_at"] = pd.to_datetime(df["extracted_at"])
    df = df.drop_duplicates()
    logging.info("Data transformation completed successfully")
    return df

@task(retries = 4, retry_delay_seconds = 10)
def load(df):
    secret_block = Secret.load("secret-thing")
    username_password = secret_block.get()
    logging.info("Loading data into PostgreSQL database")
    engine = create_engine(
        f"postgresql+psycopg2://{username_password}@Localhost:5432/ETL_Database"
    )
    df.to_sql("coin_crypto_data", engine, if_exists='append', index=False)
    logging.info("Data loaded to database successfully")

@flow(retries = 4, retry_delay_seconds = 10)
def etl_flow():
        data = extract()
        df = transform(data)
        load(df)
if __name__ == "__main__":
    etl_flow()
