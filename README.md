# Airflow Practice

A hands-on Apache Airflow project that demonstrates how to build an end-to-end ETL (Extract, Transform, Load) pipeline using Python, PostgreSQL, and Apache Airflow.

This project is designed for beginners learning data engineering and introduces many of the core concepts used in production data pipelines, including data validation, transformation, database loading, quality checks, workflow orchestration, and task dependencies.

---

## Overview

ABC Retail operates an online store where customers continuously create accounts throughout the day.

Every five minutes, the data engineering team runs an automated ETL pipeline to prepare the latest customer records for analytics.

The pipeline performs the following tasks:

```text
Generate Customer Data
          │
          ▼
Validate Data
          │
          ▼
Transform Data
          │
          ▼
Load into PostgreSQL
          │
          ▼
Run Quality Checks
          │
          ▼
Notify on Success
```

The objective is to ensure that only clean, validated data is loaded into the company's analytical database.

---

## Features

* Generate realistic customer data using Faker
* Validate incoming data before processing
* Transform customer records using pandas
* Load data into PostgreSQL
* Execute data quality checks
* Orchestrate the workflow using Apache Airflow
* Demonstrate task dependencies with Airflow operators
* Configure the application using environment variables

---

## Technologies

* Python
* Apache Airflow
* PostgreSQL
* pandas
* SQLAlchemy
* Faker
* python-dotenv

---

## Repository Structure

```text
airflow-practice/
│
├── dags/
│   └── customers_postgres_pipeline.py
│
├── data/
│   └── .gitkeep
│
├── sql/
│   └── create_customers_table.sql
│
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Pipeline Description

### 1. Generate Customer Data

The pipeline begins by generating sample customer records using the Faker library.

Each record contains fields such as:

* Name
* Age
* Street Address
* City
* State
* Zip Code
* Longitude
* Latitude

The generated records are written to a CSV file.

---

### 2. Validate the Data

Before any processing occurs, the pipeline validates the dataset by checking that:

* the CSV file is not empty
* every customer has a name
* all ages fall within the expected range

If validation fails, the pipeline stops immediately.

---

### 3. Transform the Data

The transformation stage prepares the data for loading by:

* converting column names to lowercase
* converting city names to uppercase

Additional transformations can easily be added as the project grows.

---

### 4. Load into PostgreSQL

After the data has been validated and transformed, it is loaded into the `customers` table in PostgreSQL using SQLAlchemy.

---

### 5. Run Quality Checks

Once the load completes, the pipeline verifies that:

* records were successfully inserted
* no customer names are missing

If any quality check fails, the DAG is marked as failed.

---

### 6. Notify on Success

The final task simply prints a success message to demonstrate how notification tasks fit into an Airflow workflow.

---

## Prerequisites

Before running this project, ensure you have:

* Python 3.11 or later
* Docker and Docker Compose
* Git

---

## Installation

### Clone the repository

```bash
git clone https://github.com/<your-username>/airflow-practice.git
cd airflow-practice
```

---

### Create a virtual environment

Linux/macOS

```bash
python -m venv .venv
source .venv/bin/activate
```

Windows

```powershell
python -m venv .venv
.venv\Scripts\activate
```

---

### Install dependencies

```bash
pip install -r requirements.txt
```

---

## Configuration

Copy the example environment file.

```bash
cp .env.example .env
```

Edit the values if necessary.

Example:

```text
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=workshop

DATA_DIR=/opt/airflow/data
```

---

## Create the Database Table

Run the SQL script located in the `sql` directory.

```sql
CREATE TABLE customers (
    name VARCHAR(255),
    age INTEGER,
    street VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    zipcode VARCHAR(20),
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION
);
```

---

## Running the Project

Start the Airflow environment.

```bash
docker compose up -d
```

Open Airflow in your browser.

```
http://localhost:8080
```

Default credentials

Username

```
airflow
```

Password

```
airflow
```

Enable the `customers_postgres_pipeline` DAG and trigger a run.

---

## Expected Output

After a successful execution:

* a CSV file is generated
* customer data is validated
* customer data is transformed
* records are inserted into PostgreSQL
* quality checks pass
* the DAG completes successfully

---

## Learning Objectives

This project introduces several important data engineering concepts, including:

* ETL pipelines
* Workflow orchestration
* Task dependencies
* Data validation
* Data transformation
* Database loading
* Data quality testing
* Environment variable management
* Working with PostgreSQL
* Using Apache Airflow operators

---

## Future Improvements

Possible enhancements include:

* Read data from an external API
* Use object storage such as Amazon S3
* Add email or Slack notifications
* Introduce Airflow Variables and Connections
* Containerize the entire project
* Add automated tests
* Implement branching and sensors
* Parameterize the DAG
* Add incremental data loading
* Integrate with Apache Spark

---

## License

This project is provided for educational purposes and is free to use, modify, and extend.
