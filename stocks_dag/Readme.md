# Stock Prices ETL Pipeline with Apache Airflow

This project implements a simple ETL (Extract, Transform, Load) pipeline using Apache Airflow to fetch stock market data from the Massive API and store it in a PostgreSQL database.

## Features

- Extracts daily stock price data for multiple companies
- Transforms the data into a Pandas DataFrame
- Loads the cleaned data into PostgreSQL
- Runs automatically every 5 minutes using Airflow DAG scheduling

## Tech Stack

- Python
- Apache Airflow
- Pandas
- SQLAlchemy
- PostgreSQL
- Massive API

---

# Structure

```bash
project/
│
├── stocks_dag/
│   └── stocks_prices_dag.py
│   └── README.md
```

---

# Stock Symbols Included

The pipeline currently tracks the following stocks:

- AAPL
- GOOGL
- TSLA
- NFLX
- AMZN

---

# How the Pipeline Works

## 1. Extract Phase

The DAG connects to the Massive API using the `RESTClient` and fetches daily stock data including:

- Open price
- High price
- Low price
- Close price
- Volume

## 2. Transform Phase

The extracted JSON data is converted into a Pandas DataFrame for easier processing.

Converts it into a dictionary as XComms does not pass data frames

## 3. Load Phase

The passed dictionary is converted back to a data frame

The data frame is loaded into a PostgreSQL table named `stocks_prices'. If the table does not exist, it is created automatically.

---

# Prerequisites

Before running the project, ensure you have:

- Python 3.12(recommended)
- Apache Airflow installed and running
- PostgreSQL installed and running
- Massive API key

---

# DAG Configuration

| Parameter | Value |
|---|---|
| DAG ID | `stocks_prices_dag` |
| Schedule | Every 5 minutes |
| Catchup | False |
| Start Date | 2026-04-23 |

---

# Database Table Schema

The pipeline creates and appends data with the following structure:

| Column | Type |
|---|---|
| symbol | TEXT |
| open | FLOAT |
| high | FLOAT |
| low | FLOAT |
| close | FLOAT |
| volume | BIGINT |

---

# Example Output

| symbol | open | high | low | close | volume |
|---|---|---|---|---|---|
| AAPL | 210.45 | 214.20 | 209.10 | 213.55 | 53214521 |

---

# License

This project is open-source and available under the MIT License.
