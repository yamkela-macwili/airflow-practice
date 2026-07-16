"""
Scenario

ABC Retail has an online store where customers continuously create accounts.

Every five minutes, the data engineering team runs an automated pipeline to
prepare the latest customer records for analytics.

The pipeline performs the following tasks:

    Generate customer records
            ↓
       Validate data
            ↓
      Transform data
            ↓
    Load into PostgreSQL
            ↓
    Run quality checks
            ↓
     Notify on success

The objective is to ensure that only clean, validated data is stored in the
company's analytical database.
"""
import csv
import datetime as dt
from datetime import timedelta
from pathlib import Path

import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )
)

DATA_DIR = Path(os.getenv("DATA_DIR"))
DATA_FILE = DATA_DIR / "customers.csv"


def generate_data():

    fake = Faker()

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(DATA_FILE, "w", newline="") as output:

        writer = csv.writer(output)

        writer.writerow([
            "name",
            "age",
            "street",
            "city",
            "state",
            "zipcode",
            "longitude",
            "latitude"
        ])

        for _ in range(10):

            writer.writerow([
                fake.name(),
                fake.random_int(min=18, max=80),
                fake.street_address(),
                fake.city(),
                fake.state(),
                fake.zipcode(),
                fake.longitude(),
                fake.latitude()
            ])

    print("Customer data generated.")


def validate_data():

    df = pd.read_csv(DATA_FILE)

    assert not df.empty, "CSV is empty"

    assert df["name"].isnull().sum() == 0

    assert df["age"].between(18, 80).all()

    print("Validation successful.")


def transform_data():

    df = pd.read_csv(DATA_FILE)

    df.columns = df.columns.str.lower()

    df["city"] = df["city"].str.upper()

    print(df.head())

    df.to_csv(DATA_FILE, index=False)

    print("Transformation complete.")


def load_postgres():
    df = pd.read_csv(DATA_FILE)

    df.to_sql(
        "customers",
        con=engine, 
        if_exists="append",
        index=False,
    )

    print("Loaded into PostgreSQL.")


def quality_checks():

    with engine.connect() as conn:

        total = conn.execute(
            text("SELECT COUNT(*) FROM customers")
        ).scalar()

        null_names = conn.execute(
            text(
                "SELECT COUNT(*) FROM customers WHERE name IS NULL"
            )
        ).scalar()

    print(f"Total rows: {total}")
    print(f"Null names: {null_names}")

    assert null_names == 0, "Found customers without names"

    print("Quality checks passed.")

default_args = {
    "owner": "yamkelam",
    "start_date": dt.datetime(2026, 7, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="customers_postgres_pipeline",
    default_args=default_args,
    schedule=timedelta(minutes=5),
    catchup=False,
) as dag:

    generate = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_postgres",
        python_callable=load_postgres,
    )

    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=quality_checks,
    )

    notify = BashOperator(
        task_id="notify_success",
        bash_command='echo "Pipeline completed successfully!"',
    )

    generate >> validate >> transform >> load >> quality >> notify