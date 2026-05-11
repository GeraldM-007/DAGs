# Gas Prices ETL Pipeline 

This project implements an ETL (Extract, Transform, Load) pipeline to fetch gas prices data of the state of California from collectapi.com and store it in a PostgreSQL database.
To automate the repetitive work of running the code manually each time the data is required, it uses airflow DAGs (Directed Acyclic Graphs).

## Features

- Extracts daily fuel gas prices data for the state of California
- Transforms the data into a Pandas DataFrame
- Loads the cleaned data into PostgreSQL
- Runs automatically every 10 minutes using Airflow DAG scheduling

## Tech Stack

- Python
- Apache Airflow
- Pandas
- SQLAlchemy
- PostgreSQL
- http.client

---

# Structure

```bash
project/
│
├── gas_prices_etl_dag
│   └── gas_prices.py
│   └── README.md
```

---

# How the Pipeline Works

## 1. Extract Phase

The DAG connects to the collectapi.com using the `http.client` and fetches the daily fuel gas prices across all cities in the state of California.

## 2. Transform Phase

The extracted JSON data is converted into a Pandas DataFrame for easier processing and passing to the loading function.

Converts it into a dictionary as XComms does not pass data frames

## 3. Load Phase

The passed dictionary is converted back to a data frame

The data frame is loaded into a PostgreSQL table named `california_gas_prices'. If the table does not exist, it is created automatically.

---

# Prerequisites

Before running the project, ensure you have:

- Python 3.12(recommended)
- Apache Airflow installed and running
- PostgreSQL installed and running
- API key from collectapi.com

---

# DAG Configuration

| Parameter | Value |
|---|---|
| DAG ID | `california_gas_prices_dag` |
| Schedule | Every 10 minutes |
| Catchup | False |
| Start Date | 2026-04-22 |

---

# Database Table Schema

The pipeline creates and appends data with the following structure:

| Column | Type |
|---|---|
| currency | TEXT |
| gasoline | FLOAT |
| midgrade | FLOAT |
| premium | FLOAT |
| diesel | FLOAT |
| cities | VARCHAR(200) |

---

# Example Output

| currency | gasoline | midgrade | premium | diesel | cities |
|---|---|---|---|---|---|
| usd | 5.982 | 6.210 | 6.416 | 7.454 | Bakersfield |

0       usd     5.982     6.210    6.416   7.454   
---

# License

This project is open-source and available under the MIT License.
