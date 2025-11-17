# walmart-sales-datawarehouse
Retail Data Warehouse using MySQL and Python — A complete end-to-end Data Warehouse project for retail sales analysis, featuring a star schema design, automated ETL pipeline, and analytical SQL queries for business insights.

---

# Hybrid Join Data Warehousing Project

## Project Overview
This project implements a **Hybrid Join** for a retail-like scenario. It loads master data (customers, products, stores, suppliers, dates) and transactional data into a **star schema** in MySQL. It then performs a hybrid join between streamed transactional data and customer data using an **in-memory hash table** and a disk buffer, writing enriched facts into a `fact_sales` table.

The project uses **Python**, **pandas**, **pymysql**, and **multithreading** for efficient processing.

---

## Prerequisites

1. **Python 3.10+**  
   Verify installation:
   ```bash
   python --version

## VS Code (recommended IDE)

#### Install required libraries

#### pip install pandas pymysql


MySQL Server running locally or remotely.

## Project files:

customer_master_data.csv

product_master_data.csv

transactional_data.csv

datawearhouse.sql
