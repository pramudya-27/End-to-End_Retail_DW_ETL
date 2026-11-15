# 🛒 End-to-End Retail Data Warehouse & ETL Pipeline (Star Schema)

![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue?logo=python&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-8.0-orange?logo=mysql&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.2+-150458?logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0+-red?logo=sqlalchemy&logoColor=white)
![Data Engineering](https://img.shields.io/badge/Architecture-Kimball%20Star%20Schema-success)
![Data Integrity](https://img.shields.io/badge/Data%20Reconciliation-100%25%20Verified-brightgreen)

> **Enterprise-grade, modular ETL pipeline in Python and MySQL 8.0 designed to extract, transform, and load over 526,000+ transactional e-commerce records into an optimized dimensional Star Schema for Business Intelligence (BI) and OLAP analytics.**

---

## 📌 1. Project Overview & Business Value

In modern e-commerce organizations, transactional systems (OLTP) are optimized for low-latency writes and operational lookups, but are ill-suited for complex analytical reporting and business intelligence.

This project establishes a **production-ready ETL pipeline and Star Schema Data Warehouse** built from a real-world multi-country e-commerce dataset containing **526,052 transaction records**.

### Key Engineering Objectives:

- **Zero Data Loss Guarantee**: Transactional records with missing customer identifiers (`133,320` rows) are strictly preserved using the **Kimball Unknown Dimension Member pattern**, ensuring 100% financial and transactional completeness.
- **High-Throughput Batch Ingestion**: Ingests **526,052 fact records in ~60 seconds** (~8,500 rows/second) using memory-efficient chunked bulk transactions.
- **Strict Referential Integrity**: Enforces relational database constraints (Foreign Keys and Primary Keys) with zero orphaned records.
- **Automated Data Quality & Reconciliation**: Validates financial aggregates, row counts, and unique entity counts between source CSV and Data Warehouse after every load.
- **Idempotent & Controlled Operations**: Safe re-execution with controlled table resets without risking unrelated production databases.

---

## 🏗️ 2. Star Schema Architecture

The data warehouse follows **Ralph Kimball’s Dimensional Modeling methodology**, featuring a central **Fact Table** connected to four **Dimension Tables** in a clean 1-to-many relationship:

```
                  +---------------------------+
                  |       dim_customer        |
                  +---------------------------+
                  | PK  CustomerKey : int     |
                  |     CustomerID  : int     |
                  +-------------+-------------+
                                |
                                | (1:N)
                                v
+-----------------------+ +-----+---------------------+ +-----------------------+
|       dim_date        | |        fact_sales         | |      dim_country      |
+-----------------------+ +---------------------------+ +-----------------------+
| PK  DateKey   : int   | | PK  SalesKey    : bigint  | | PK  CountryKey : int  |
|     FullDate  : date  | |     InvoiceNo   : varchar | |     CountryName: str  |
|     Day       : int   | | FK  DateKey     : int     | +-----------+-----------+
|     Month     : int   | | FK  ProductKey  : int     |             |
|     MonthName : str   | | FK  CustomerKey : int     |             | (1:N)
|     Quarter   : int   | | FK  CountryKey  : int     | <-----------+
|     Year      : int   | |     Quantity    : int     |
+-----------+-----------+ |     UnitPrice   : decimal |
            |             |     GrossSales  : decimal |
            | (1:N)       +-------------+-------------+
            +-------------------------> |
                                        | (1:N)
                                        ^
                          +-------------+-------------+
                          |        dim_product        |
                          +---------------------------+
                          | PK  ProductKey  : int     |
                          |     StockCode   : varchar |
                          |     Description : varchar |
                          +---------------------------+
```

### Data Warehouse Dictionary:

| Table Name         | Table Type | Natural Key              | Surrogate Key                      | Grain / Description                                          |
| :----------------- | :--------- | :----------------------- | :--------------------------------- | :----------------------------------------------------------- |
| **`fact_sales`**   | Fact Table | `InvoiceNo` (Degenerate) | `SalesKey` (BIGINT AUTO_INCREMENT) | **One row per product item in an invoice (invoice line)**    |
| **`dim_date`**     | Dimension  | `FullDate`               | `DateKey` (INT: `YYYYMMDD`)        | Calendar dimension tracking day, month, quarter, and year    |
| **`dim_product`**  | Dimension  | `StockCode`              | `ProductKey` (INT AUTO_INCREMENT)  | Unique catalog products with standardized descriptions       |
| **`dim_customer`** | Dimension  | `CustomerID`             | `CustomerKey` (INT AUTO_INCREMENT) | Registered customers + dedicated **Unknown Customer** member |
| **`dim_country`**  | Dimension  | `CountryName`            | `CountryKey` (INT AUTO_INCREMENT)  | Distinct geographic delivery countries                       |

---

## 💡 3. Key Data Engineering Challenges & Solutions

### 1. Handling Missing Customer Identifiers (`NULL` Handling)

- **Challenge**: 133,320 valid transactions (25.3% of total revenue) do not have a logged `CustomerID` (guest checkouts or anonymized transactions). Deleting them would result in severe financial distortion.
- **Solution**: Implemented the **Kimball Unknown Dimension Member pattern**. `dim_customer` creates a reserved record (`CustomerKey = 1`, `CustomerID = NULL`). All anonymous sales link to `CustomerKey = 1`, maintaining 100% revenue integrity while preserving Foreign Key constraints.

### 2. Case-Sensitive StockCode Collision Resolution

- **Challenge**: MySQL's standard collations (`utf8mb4_0900_ai_ci` or `utf8mb4_unicode_ci`) are case-insensitive. In the retail dataset, specific product stock codes distinguish variants by letter case (e.g., uppercase vs. lowercase suffixes). Under a standard collation, these codes collided on `UNIQUE KEY`, causing dimension insertion drops and fact table foreign key violation errors.
- **Solution**: Explicitly configured `StockCode VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL UNIQUE`. This guarantees exact binary-level distinctness, allowing all **3,941 unique products** to be indexed and matched flawlessly.

### 3. Memory & Network-Efficient Bulk Loading

- **Challenge**: Inserting 526,052 rows one-by-one or via unbuffered bulk operations causes timeouts, memory bloat, and lock contention.
- **Solution**: Implemented parameterized batch execution via `PyMySQL` with a calibrated `BATCH_SIZE = 20,000`. Converted pandas DataFrames to low-overhead Python tuple generators, achieving ingestion speeds of **~8,500 rows/second** and loading the entire dataset in under **65 seconds**.

---

## 📁 4. Project Directory Structure

```
kecerdasan_bisnis/
│
├── data/
│   └── Online_Retail_Hasil_Cleaning.csv   <- Source dataset (526,052 rows)
│
├── staging/                               <- Temporary intermediate data store
│   ├── extracted_raw.csv                  <- Extraction artifact
│   ├── dim_date.csv                       <- Dimension artifact
│   ├── dim_product.csv                    <- Dimension artifact
│   ├── dim_customer.csv                   <- Dimension artifact
│   ├── dim_country.csv                    <- Dimension artifact
│   └── fact_sales.csv                     <- Fact table artifact
│
├── config.py                              <- Centralized environment & database configuration
├── extract.py                             <- Phase 1: Data extraction & schema validation
├── transform.py                           <- Phase 2: Dimensional modeling & key mapping
├── load.py                                <- Phase 3: DDL creation, batch loading, & reconciliation
├── main.py                                <- End-to-end pipeline orchestrator
│
├── validation_queries.sql                 <- SQL test suite & BI analytical queries
├── requirements.txt                       <- Python dependencies
├── .env.example                           <- Environment configuration template
├── .env                                   <- Active local environment file
└── README.md                              <- Project documentation
```

---

## ⚙️ 5. Prerequisites & Quick Start

### A. Environment Requirements

- **Operating System**: Windows 10/11, macOS, or Linux
- **Python**: Version 3.10, 3.11, 3.12, or 3.13
- **Database**: MySQL 8.0 (Localhost, Docker, Laragon, or XAMPP)

### B. Virtual Environment Setup

Open terminal / PowerShell in the project directory:

```powershell
# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Install required dependencies
pip install -r requirements.txt
```

### C. Database Credentials Configuration

Copy `.env.example` to `.env` and set your MySQL credentials:

```ini
# MySQL 8.0 Configuration
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=
DB_NAME=online_retail_dw

# File Path & Staging Directory
DATA_FILE_PATH=data/Online_Retail_Hasil_Cleaning.csv
STAGING_DIR=staging

# Execution Parameters
BATCH_SIZE=20000
RESET_TABLES=true
```

---

## 🚀 6. Running the ETL Pipeline

You can run the pipeline either as an **all-in-one execution** or **step-by-step**:

### Option 1: Unified Execution (Recommended)

Run the entire pipeline end-to-end with a single command:

```powershell
python main.py
```

_This command automatically executes Extract ➔ Transform ➔ Load ➔ Validation and outputs an executive execution summary._

---

### Option 2: Step-by-Step Modular Execution

```powershell
# Step 1: Extract data from CSV and validate schema
python extract.py

# Step 2: Transform data into dimensional tables and surrogate keys
python transform.py

# Step 3: Create database schema, batch load, and reconcile metrics
python load.py
```

---

## 📊 7. Automated Data Reconciliation & Quality Audit

Every pipeline run concludes with an **automated reconciliation audit** comparing source CSV statistics against SQL aggregates calculated live from the data warehouse:

```
=================================================================
>>> TAHAP VALIDASI ETL & DATA WAREHOUSE RECONCILIATION
=================================================================
Indikator Metrik             | Sumber CSV       | Data Warehouse   | Status
---------------------------------------------------------------------------
Jumlah Baris Penjualan       | 526052           | 526052           | [PASSED]
Total Quantity               | 5645017          | 5645017          | [PASSED]
Total GrossSales             | 10,642,110.80    | 10,642,110.80    | [PASSED]
Jumlah Invoice Unik          | 20726            | 20726            | [PASSED]
Jumlah Produk Unik           | 3941             | 3941             | [PASSED]
Jumlah Pelanggan Non-Null    | 4339             | 4339             | [PASSED]
---------------------------------------------------------------------------

[VALIDASI TABEL DIMENSI]
  - dim_date     : 305 entri tanggal
  - dim_product  : 3,941 entri produk unik
  - dim_customer : 4,340 entri (4,339 terdaftar + 1 Unknown)
  - dim_country  : 38 entri negara

[VALIDASI INTEGRITAS FOREIGN KEY (ORPHAN KEY CHECK)]
  - Orphan Date FK     : 0 (Harus 0)
  - Orphan Product FK  : 0 (Harus 0)
  - Orphan Customer FK : 0 (Harus 0)
  - Orphan Country FK  : 0 (Harus 0)

[SUCCESS] SELURUH PROSES VALIDASI ETL BERHASIL 100%!
Data Warehouse Star Schema telah terbentuk sempurna dan konsisten.
=================================================================
```

---

## 📈 8. Sample Business Intelligence (BI) Analytics Queries

Connect your preferred BI client (DBeaver, Power BI, Tableau, or MySQL Workbench) to `online_retail_dw` and execute queries from [validation_queries.sql](validation_queries.sql):

### A. Top 5 Revenue-Generating Products

```sql
SELECT
    dp.StockCode,
    dp.Description AS Product_Name,
    SUM(fs.Quantity) AS Total_Units_Sold,
    SUM(fs.GrossSales) AS Total_Revenue
FROM fact_sales fs
JOIN dim_product dp ON fs.ProductKey = dp.ProductKey
GROUP BY dp.ProductKey, dp.StockCode, dp.Description
ORDER BY Total_Revenue DESC
LIMIT 5;
```

### B. Monthly Sales Trend Analysis

```sql
SELECT
    dd.Year,
    dd.Month,
    dd.MonthName,
    COUNT(DISTINCT fs.InvoiceNo) AS Order_Count,
    SUM(fs.Quantity) AS Total_Volume,
    SUM(fs.GrossSales) AS Total_Revenue
FROM fact_sales fs
JOIN dim_date dd ON fs.DateKey = dd.DateKey
GROUP BY dd.Year, dd.Month, dd.MonthName
ORDER BY dd.Year ASC, dd.Month ASC;
```

### C. Top 5 International Markets by Sales Volume

```sql
SELECT
    dcn.CountryName,
    COUNT(DISTINCT fs.InvoiceNo) AS Unique_Invoices,
    SUM(fs.Quantity) AS Units_Sold,
    SUM(fs.GrossSales) AS Total_Revenue
FROM fact_sales fs
JOIN dim_country dcn ON fs.CountryKey = dcn.CountryKey
GROUP BY dcn.CountryKey, dcn.CountryName
ORDER BY Total_Revenue DESC
LIMIT 5;
```

---

## 🛠️ 9. Technology Stack

- **Core Language**: Python 3.13
- **Data Manipulation & ETL**: `pandas`, `numpy`
- **Database Engine**: MySQL 8.0 (InnoDB)
- **Database Connectivity**: `PyMySQL`, `SQLAlchemy 2.0+`
- **Security & Config**: `python-dotenv`, `cryptography`
- **Architecture**: Ralph Kimball Dimensional Star Schema Modeling

---

## 📜 10. License & Attribution

This project is licensed under the **MIT License** — feel free to use, modify, and distribute this codebase for educational and commercial data engineering implementations.
