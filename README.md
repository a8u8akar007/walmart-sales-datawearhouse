# Hybrid Join Data Warehousing Project

This project implements a **Hybrid Join strategy** within an automated ETL pipeline to load and enrich retail sales data into a MySQL Star Schema Data Warehouse. The goal is to efficiently process high-volume transactional data by leveraging in-memory hash tables for dimension lookups.

## Project Overview

This solution simulates a retail sales analysis scenario:

- **Star Schema Design**: Data is loaded into a Star Schema featuring `dim_customer`, `dim_product`, and the central `fact_sales` table.
- **Hybrid Join**: Dimension data (customers, products) is loaded into memory (as Python dictionaries/hash tables). Transactional data is streamed in chunks, and dimension attributes are looked up in-memory for fast enrichment before being written to the `fact_sales` table.
- **Technology Stack**: Python, pandas, and pymysql are used for ETL, while MySQL serves as the Data Warehouse repository.

## Technology Stack

- **Python 3.10+** with pandas, pymysql
- **MySQL Server**
- **VS Code** (Recommended IDE)


## Installation and Setup

### Prerequisites

- Python 3.10+
- MySQL Server instance (running and accessible)

### 1. Install Required Libraries

```bash
pip install pandas pymysql

# DATABASE CONFIGURATION (Update These Values)
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',                    # Your MySQL Username
    'password': 'your_secure_password', # Your MySQL Password
    'database': 'retail_dw'
}
