"""
Module: load.py
Deskripsi: Tahap 3 ETL - Memuat data dimensi dan fakta ke Data Warehouse.
Mendukung MySQL 8.0 (localhost) dan PostgreSQL / Supabase Cloud secara otomatis.
Menerapkan batch insert & streaming COPY, integritas referensial (PK/FK),
serta validasi rekonsiliasi data otomatis.
"""

import sys
import time
from pathlib import Path
from typing import Dict, Optional
import pandas as pd

from config import (
    DB_TYPE,
    DB_NAME,
    BATCH_SIZE,
    RESET_TABLES,
    STAGING_DIR,
    get_db_connection,
)


def create_database_if_not_exists() -> None:
    """Memeriksa database tujuan."""
    if DB_TYPE == "postgres":
        print(f"[INFO] Target: PostgreSQL / Supabase Cloud (Database: `{DB_NAME}`).")
        # Pada Supabase/PostgreSQL, database 'postgres' sudah ada secara default.
        return

    print(f"[INFO] Memeriksa dan membuat database MySQL `{DB_NAME}` jika belum ada...")
    conn = get_db_connection(use_database=False)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            )
        conn.commit()
        print(f"[INFO] Database `{DB_NAME}` siap digunakan.")
    finally:
        conn.close()


def prepare_tables(conn, reset: bool = True) -> None:
    """
    Mempersiapkan tabel Star Schema di database (MySQL atau PostgreSQL/Supabase).
    Jika reset=True, tabel proyek ini di-drop secara terkontrol
    sesuai urutan foreign key agar proses idempoten dan aman dijalankan ulang.
    """
    with conn.cursor() as cur:
        if reset:
            print("[INFO] RESET_TABLES=True: Menghapus tabel lama proyek ini...")
            if DB_TYPE == "postgres":
                cur.execute("DROP TABLE IF EXISTS fact_sales CASCADE;")
                cur.execute("DROP TABLE IF EXISTS dim_date CASCADE;")
                cur.execute("DROP TABLE IF EXISTS dim_product CASCADE;")
                cur.execute("DROP TABLE IF EXISTS dim_customer CASCADE;")
                cur.execute("DROP TABLE IF EXISTS dim_country CASCADE;")
            else:
                cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cur.execute("DROP TABLE IF EXISTS fact_sales;")
                cur.execute("DROP TABLE IF EXISTS dim_date;")
                cur.execute("DROP TABLE IF EXISTS dim_product;")
                cur.execute("DROP TABLE IF EXISTS dim_customer;")
                cur.execute("DROP TABLE IF EXISTS dim_country;")
                cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
            print("[INFO] Tabel lama berhasil dibersihkan.")

        print("[INFO] Membuat tabel-tabel Star Schema...")

        if DB_TYPE == "postgres":
            # DDL PostgreSQL / Supabase
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_date (
                DateKey INT PRIMARY KEY,
                FullDate DATE NOT NULL,
                Day INT NOT NULL,
                Month INT NOT NULL,
                MonthName VARCHAR(20) NOT NULL,
                Quarter INT NOT NULL,
                Year INT NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_dimdate_fulldate ON dim_date (FullDate);")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_product (
                ProductKey SERIAL PRIMARY KEY,
                StockCode VARCHAR(20) NOT NULL UNIQUE,
                Description VARCHAR(255) NOT NULL
            );
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_customer (
                CustomerKey SERIAL PRIMARY KEY,
                CustomerID INT NULL UNIQUE
            );
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_country (
                CountryKey SERIAL PRIMARY KEY,
                CountryName VARCHAR(100) NOT NULL UNIQUE
            );
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                SalesKey BIGSERIAL PRIMARY KEY,
                InvoiceNo VARCHAR(20) NOT NULL,
                DateKey INT NOT NULL REFERENCES dim_date(DateKey) ON UPDATE CASCADE,
                ProductKey INT NOT NULL REFERENCES dim_product(ProductKey) ON UPDATE CASCADE,
                CustomerKey INT NOT NULL REFERENCES dim_customer(CustomerKey) ON UPDATE CASCADE,
                CountryKey INT NOT NULL REFERENCES dim_country(CountryKey) ON UPDATE CASCADE,
                Quantity INT NOT NULL,
                UnitPrice NUMERIC(10, 2) NOT NULL,
                GrossSales NUMERIC(12, 2) NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_invoice ON fact_sales (InvoiceNo);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON fact_sales (DateKey);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_product ON fact_sales (ProductKey);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_customer ON fact_sales (CustomerKey);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_country ON fact_sales (CountryKey);")
        else:
            # DDL MySQL 8.0
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_date (
                DateKey INT PRIMARY KEY,
                FullDate DATE NOT NULL,
                Day INT NOT NULL,
                Month INT NOT NULL,
                MonthName VARCHAR(20) NOT NULL,
                Quarter INT NOT NULL,
                Year INT NOT NULL,
                INDEX idx_fulldate (FullDate)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_product (
                ProductKey INT AUTO_INCREMENT PRIMARY KEY,
                StockCode VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                Description VARCHAR(255) NOT NULL,
                UNIQUE KEY uq_stock_code (StockCode)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_customer (
                CustomerKey INT AUTO_INCREMENT PRIMARY KEY,
                CustomerID INT NULL,
                UNIQUE KEY uq_customer_id (CustomerID)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_country (
                CountryKey INT AUTO_INCREMENT PRIMARY KEY,
                CountryName VARCHAR(100) NOT NULL,
                UNIQUE KEY uq_country_name (CountryName)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                SalesKey BIGINT AUTO_INCREMENT PRIMARY KEY,
                InvoiceNo VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                DateKey INT NOT NULL,
                ProductKey INT NOT NULL,
                CustomerKey INT NOT NULL,
                CountryKey INT NOT NULL,
                Quantity INT NOT NULL,
                UnitPrice DECIMAL(10, 2) NOT NULL,
                GrossSales DECIMAL(12, 2) NOT NULL,
                CONSTRAINT fk_sales_date FOREIGN KEY (DateKey) REFERENCES dim_date(DateKey) ON UPDATE CASCADE,
                CONSTRAINT fk_sales_product FOREIGN KEY (ProductKey) REFERENCES dim_product(ProductKey) ON UPDATE CASCADE,
                CONSTRAINT fk_sales_customer FOREIGN KEY (CustomerKey) REFERENCES dim_customer(CustomerKey) ON UPDATE CASCADE,
                CONSTRAINT fk_sales_country FOREIGN KEY (CountryKey) REFERENCES dim_country(CountryKey) ON UPDATE CASCADE,
                INDEX idx_invoice (InvoiceNo),
                INDEX idx_date (DateKey),
                INDEX idx_product (ProductKey),
                INDEX idx_customer (CustomerKey),
                INDEX idx_country (CountryKey)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

    conn.commit()
    print(f"[INFO] Seluruh tabel Star Schema siap pada engine {DB_TYPE.upper()}.")


def load_dimensions(conn, dfs: Dict[str, pd.DataFrame]) -> None:
    """Memasukkan data ke seluruh tabel dimensi."""
    print("\n[INFO] Memuat data ke tabel dimensi...")

    with conn.cursor() as cur:
        # 1. DimDate
        t0 = time.time()
        date_records = [
            (
                int(r.DateKey),
                str(r.FullDate),
                int(r.Day),
                int(r.Month),
                str(r.MonthName),
                int(r.Quarter),
                int(r.Year),
            )
            for r in dfs["dim_date"].itertuples(index=False)
        ]
        if DB_TYPE == "postgres":
            query_date = """
            INSERT INTO dim_date (DateKey, FullDate, Day, Month, MonthName, Quarter, Year)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (DateKey) DO UPDATE SET FullDate=EXCLUDED.FullDate;
            """
        else:
            query_date = """
            INSERT INTO dim_date (DateKey, FullDate, Day, Month, MonthName, Quarter, Year)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE FullDate=VALUES(FullDate);
            """
        cur.executemany(query_date, date_records)
        print(f"       -> dim_date    : {len(date_records):,} baris dimuat ({time.time()-t0:.2f}s)")

        # 2. DimProduct
        t0 = time.time()
        product_records = [
            (int(r.ProductKey), str(r.StockCode), str(r.Description))
            for r in dfs["dim_product"].itertuples(index=False)
        ]
        if DB_TYPE == "postgres":
            query_prod = """
            INSERT INTO dim_product (ProductKey, StockCode, Description)
            VALUES (%s, %s, %s)
            ON CONFLICT (StockCode) DO UPDATE SET Description=EXCLUDED.Description;
            """
        else:
            query_prod = """
            INSERT INTO dim_product (ProductKey, StockCode, Description)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE Description=VALUES(Description);
            """
        cur.executemany(query_prod, product_records)
        print(f"       -> dim_product : {len(product_records):,} baris dimuat ({time.time()-t0:.2f}s)")

        # 3. DimCustomer
        t0 = time.time()
        customer_records = []
        for r in dfs["dim_customer"].itertuples(index=False):
            cid = None if pd.isna(r.CustomerID) else int(r.CustomerID)
            customer_records.append((int(r.CustomerKey), cid))
        
        if DB_TYPE == "postgres":
            query_cust = """
            INSERT INTO dim_customer (CustomerKey, CustomerID)
            VALUES (%s, %s)
            ON CONFLICT (CustomerID) DO UPDATE SET CustomerID=EXCLUDED.CustomerID;
            """
        else:
            query_cust = """
            INSERT INTO dim_customer (CustomerKey, CustomerID)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE CustomerID=VALUES(CustomerID);
            """
        cur.executemany(query_cust, customer_records)
        print(f"       -> dim_customer: {len(customer_records):,} baris dimuat ({time.time()-t0:.2f}s)")

        # 4. DimCountry
        t0 = time.time()
        country_records = [
            (int(r.CountryKey), str(r.CountryName))
            for r in dfs["dim_country"].itertuples(index=False)
        ]
        if DB_TYPE == "postgres":
            query_ctry = """
            INSERT INTO dim_country (CountryKey, CountryName)
            VALUES (%s, %s)
            ON CONFLICT (CountryName) DO UPDATE SET CountryName=EXCLUDED.CountryName;
            """
        else:
            query_ctry = """
            INSERT INTO dim_country (CountryKey, CountryName)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE CountryName=VALUES(CountryName);
            """
        cur.executemany(query_ctry, country_records)
        print(f"       -> dim_country : {len(country_records):,} baris dimuat ({time.time()-t0:.2f}s)")

    conn.commit()


def load_facts(
    conn,
    fact_df: pd.DataFrame,
    staging_dir: Path,
    batch_size: int = BATCH_SIZE,
) -> None:
    """
    Memasukkan data ke fact_sales:
    - Pada Supabase / PostgreSQL: Menggunakan PostgreSQL streaming COPY (sangat cepat over cloud WAN)
      atau fallback ke multi-row VALUES batching.
    - Pada MySQL: Menggunakan chunked executemany.
    """
    total_records = len(fact_df)
    start_load_time = time.time()

    if DB_TYPE == "postgres":
        csv_path = staging_dir / "fact_sales.csv"
        if csv_path.exists():
            print(f"\n[INFO] Menggunakan PostgreSQL Streaming COPY untuk Supabase Cloud ({total_records:,} baris)...")
            with conn.cursor() as cur:
                with open(csv_path, "r", encoding="utf-8") as f:
                    cur.copy_expert(
                        """
                        COPY fact_sales (
                            InvoiceNo, DateKey, ProductKey, CustomerKey, CountryKey,
                            Quantity, UnitPrice, GrossSales
                        )
                        FROM STDIN WITH (FORMAT csv, HEADER true);
                        """,
                        f,
                    )
                conn.commit()
            total_load_time = time.time() - start_load_time
            print(f"[INFO] Streaming COPY ke Supabase SELESAI dalam {total_load_time:.2f} detik "
                  f"({total_records / max(total_load_time, 0.01):.0f} baris/detik)!")
            return

    # Fallback / MySQL batch insert
    print(f"\n[INFO] Memuat {total_records:,} baris ke fact_sales (Batch Size: {batch_size:,})...")
    fact_tuples = [
        (
            str(r.InvoiceNo),
            int(r.DateKey),
            int(r.ProductKey),
            int(r.CustomerKey),
            int(r.CountryKey),
            int(r.Quantity),
            float(r.UnitPrice),
            float(r.GrossSales),
        )
        for r in fact_df.itertuples(index=False)
    ]

    insert_query = """
    INSERT INTO fact_sales (
        InvoiceNo, DateKey, ProductKey, CustomerKey, CountryKey, Quantity, UnitPrice, GrossSales
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    total_chunks = (total_records + batch_size - 1) // batch_size
    with conn.cursor() as cur:
        for idx in range(total_chunks):
            chunk_start = idx * batch_size
            chunk_end = min(chunk_start + batch_size, total_records)
            chunk = fact_tuples[chunk_start:chunk_end]

            cur.executemany(insert_query, chunk)
            conn.commit()

            progress_pct = (chunk_end / total_records) * 100
            elapsed = time.time() - start_load_time
            print(f"       [Batch {idx+1:>2}/{total_chunks:>2}] "
                  f"Dimuat: {chunk_end:>7,} / {total_records:,} ({progress_pct:5.1f}%) "
                  f"| Elapsed: {elapsed:5.1f}s")

    total_load_time = time.time() - start_load_time
    print(f"[INFO] Pengisian fact_sales SELESAI dalam {total_load_time:.2f} detik "
          f"({total_records / max(total_load_time, 0.01):.0f} baris/detik)")


def validate_etl(conn, source_df: pd.DataFrame) -> bool:
    """
    Melakukan validasi rekonsiliasi data antara sumber data dan Data Warehouse:
    Kompatibel dengan MySQL dan PostgreSQL / Supabase.
    """
    print("\n" + "=" * 65)
    print(">>> TAHAP VALIDASI ETL & DATA WAREHOUSE RECONCILIATION")
    print(f"    Target DBMS: {DB_TYPE.upper()} ({DB_NAME})")
    print("=" * 65)

    with conn.cursor() as cur:
        # Metrik FactSales
        cur.execute("""
        SELECT
            COUNT(*) AS dw_fact_rows,
            COALESCE(SUM(Quantity), 0) AS dw_total_quantity,
            COALESCE(SUM(GrossSales), 0) AS dw_total_gross_sales,
            COUNT(DISTINCT InvoiceNo) AS dw_unique_invoices
        FROM fact_sales;
        """)
        row = cur.fetchone()
        dw_fact_res = {k.lower(): v for k, v in row.items()} if isinstance(row, dict) else {
            "dw_fact_rows": row[0],
            "dw_total_quantity": row[1],
            "dw_total_gross_sales": row[2],
            "dw_unique_invoices": row[3],
        }

        # Metrik Dimensi
        cur.execute("SELECT COUNT(*) FROM dim_date;")
        dw_dates = cur.fetchone()
        dw_dates = list(dw_dates.values())[0] if isinstance(dw_dates, dict) else dw_dates[0]

        cur.execute("SELECT COUNT(*) FROM dim_product;")
        dw_products = cur.fetchone()
        dw_products = list(dw_products.values())[0] if isinstance(dw_products, dict) else dw_products[0]

        cur.execute("SELECT COUNT(*) FROM dim_customer;")
        dw_customers = cur.fetchone()
        dw_customers = list(dw_customers.values())[0] if isinstance(dw_customers, dict) else dw_customers[0]

        cur.execute("SELECT COUNT(*) FROM dim_country;")
        dw_countries = cur.fetchone()
        dw_countries = list(dw_countries.values())[0] if isinstance(dw_countries, dict) else dw_countries[0]

        cur.execute("SELECT COUNT(CustomerID) FROM dim_customer WHERE CustomerID IS NOT NULL;")
        dw_customers_non_null = cur.fetchone()
        dw_customers_non_null = list(dw_customers_non_null.values())[0] if isinstance(dw_customers_non_null, dict) else dw_customers_non_null[0]

        # Cek Orphan Foreign Key
        cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM fact_sales fs LEFT JOIN dim_date dd ON fs.DateKey = dd.DateKey WHERE dd.DateKey IS NULL) AS orphan_date,
            (SELECT COUNT(*) FROM fact_sales fs LEFT JOIN dim_product dp ON fs.ProductKey = dp.ProductKey WHERE dp.ProductKey IS NULL) AS orphan_product,
            (SELECT COUNT(*) FROM fact_sales fs LEFT JOIN dim_customer dc ON fs.CustomerKey = dc.CustomerKey WHERE dc.CustomerKey IS NULL) AS orphan_customer,
            (SELECT COUNT(*) FROM fact_sales fs LEFT JOIN dim_country dcn ON fs.CountryKey = dcn.CountryKey WHERE dcn.CountryKey IS NULL) AS orphan_country;
        """)
        orph = cur.fetchone()
        if isinstance(orph, dict):
            # Normalisasi kunci ke lowercase
            orphan_res = {k.lower(): v for k, v in orph.items()}
        else:
            orphan_res = {
                "orphan_date": orph[0],
                "orphan_product": orph[1],
                "orphan_customer": orph[2],
                "orphan_country": orph[3],
            }

    # Metrik Sumber CSV
    src_total_rows = len(source_df)
    src_total_qty = int(source_df["Quantity"].sum())
    src_total_gross = round(float(source_df["GrossSales"].sum()), 2)
    src_unique_invoices = source_df["InvoiceNo"].nunique()
    src_unique_products = source_df["StockCode"].nunique()
    src_unique_customers = source_df["CustomerID"].dropna().nunique()

    dw_total_rows = int(dw_fact_res["dw_fact_rows"])
    dw_total_qty = int(dw_fact_res["dw_total_quantity"])
    dw_total_gross = round(float(dw_fact_res["dw_total_gross_sales"]), 2)
    dw_unique_invoices = int(dw_fact_res["dw_unique_invoices"])

    # Tabel Perbandingan Rekonsiliasi
    checks = [
        ("Jumlah Baris Penjualan", src_total_rows, dw_total_rows, src_total_rows == dw_total_rows),
        ("Total Quantity", src_total_qty, dw_total_qty, src_total_qty == dw_total_qty),
        ("Total GrossSales", f"{src_total_gross:,.2f}", f"{dw_total_gross:,.2f}", abs(src_total_gross - dw_total_gross) < 0.05),
        ("Jumlah Invoice Unik", src_unique_invoices, dw_unique_invoices, src_unique_invoices == dw_unique_invoices),
        ("Jumlah Produk Unik", src_unique_products, dw_products, src_unique_products == dw_products),
        ("Jumlah Pelanggan Non-Null", src_unique_customers, dw_customers_non_null, src_unique_customers == dw_customers_non_null),
    ]

    print(f"{'Indikator Metrik':<28} | {'Sumber CSV':<16} | {'Data Warehouse':<16} | {'Status'}")
    print("-" * 75)
    all_passed = True
    for name, s_val, d_val, ok in checks:
        status_str = "[PASSED]" if ok else "[FAILED]"
        if not ok:
            all_passed = False
        print(f"{name:<28} | {str(s_val):<16} | {str(d_val):<16} | {status_str}")

    print("-" * 75)
    print("\n[VALIDASI TABEL DIMENSI]")
    print(f"  - dim_date     : {dw_dates:,} entri tanggal")
    print(f"  - dim_product  : {dw_products:,} entri produk unik")
    print(f"  - dim_customer : {dw_customers:,} entri ({dw_customers_non_null:,} terdaftar + 1 Unknown)")
    print(f"  - dim_country  : {dw_countries:,} entri negara")

    print("\n[VALIDASI INTEGRITAS FOREIGN KEY (ORPHAN KEY CHECK)]")
    orphan_date = orphan_res["orphan_date"]
    orphan_prod = orphan_res["orphan_product"]
    orphan_cust = orphan_res["orphan_customer"]
    orphan_ctry = orphan_res["orphan_country"]

    print(f"  - Orphan Date FK     : {orphan_date} (Harus 0)")
    print(f"  - Orphan Product FK  : {orphan_prod} (Harus 0)")
    print(f"  - Orphan Customer FK : {orphan_cust} (Harus 0)")
    print(f"  - Orphan Country FK  : {orphan_ctry} (Harus 0)")

    fk_integrity = (orphan_date == 0 and orphan_prod == 0 and orphan_cust == 0 and orphan_ctry == 0)
    if not fk_integrity:
        all_passed = False
        print("[WARNING] Ditemukan foreign key yang tidak memiliki pasangan di tabel dimensi!")

    # Sampel JOIN
    print(f"\n[SAMPEL HASIL JOIN STAR SCHEMA ({DB_TYPE.upper()})]")
    with conn.cursor() as cur:
        sample_query = """
        SELECT
            fs.InvoiceNo,
            dd.FullDate,
            dp.StockCode,
            LEFT(dp.Description, 25) AS Description,
            COALESCE(CAST(dc.CustomerID AS CHAR(20)), 'Unknown') AS CustomerID,
            dcn.CountryName,
            fs.Quantity,
            fs.UnitPrice,
            fs.GrossSales
        FROM fact_sales fs
        JOIN dim_date dd ON fs.DateKey = dd.DateKey
        JOIN dim_product dp ON fs.ProductKey = dp.ProductKey
        JOIN dim_customer dc ON fs.CustomerKey = dc.CustomerKey
        JOIN dim_country dcn ON fs.CountryKey = dcn.CountryKey
        ORDER BY fs.SalesKey ASC
        LIMIT 5;
        """
        cur.execute(sample_query)
        rows = cur.fetchall()
        for idx, r in enumerate(rows, 1):
            if isinstance(r, dict):
                rd = {k.lower(): v for k, v in r.items()}
                inv, dt, code, desc, cid, ctry, qty, gross = (
                    rd['invoiceno'], rd['fulldate'], rd['stockcode'], rd['description'],
                    rd['customerid'], rd['countryname'], rd['quantity'], rd['grosssales']
                )
            else:
                inv, dt, code, desc, cid, ctry, qty, _, gross = r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]
            print(f"  {idx}. Inv: {inv} | Tgl: {dt} | Item: {code} ({desc}) | "
                  f"Cust: {cid} | {ctry} | Qty: {qty} | Gross: {gross}")

    print("=" * 65)
    if all_passed:
        print(f"[SUCCESS] SELURUH PROSES VALIDASI ETL BERHASIL 100% PADA {DB_TYPE.upper()}!")
        print("Data Warehouse Star Schema telah terbentuk sempurna dan konsisten.")
    else:
        print("[ERROR] VALIDASI ETL TIDAK SESUAI! Silakan periksa log perbedaan di atas.")
    print("=" * 65 + "\n")

    return all_passed


def load_data(
    transformed_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    staging_dir: Optional[Path] = None,
    reset_tables: bool = RESET_TABLES,
) -> bool:
    """
    Fungsi utama untuk tahap Load.
    Membuat database/schema, tabel, memuat dimensi, memuat fakta, dan memvalidasi rekonsiliasi.
    """
    if staging_dir is None:
        staging_dir = STAGING_DIR

    print("\n" + "=" * 65)
    print(f">>> TAHAP 3: LOADING KE {DB_TYPE.upper()} (load.py)")
    print("=" * 65)
    overall_start = time.time()

    create_database_if_not_exists()

    if transformed_dfs is None:
        print(f"[INFO] Membaca data hasil transformasi dari folder staging: {staging_dir} ...")
        transformed_dfs = {
            "dim_date": pd.read_csv(staging_dir / "dim_date.csv"),
            "dim_product": pd.read_csv(staging_dir / "dim_product.csv", dtype={"StockCode": str}),
            "dim_customer": pd.read_csv(staging_dir / "dim_customer.csv"),
            "dim_country": pd.read_csv(staging_dir / "dim_country.csv"),
            "fact_sales": pd.read_csv(staging_dir / "fact_sales.csv", dtype={"InvoiceNo": str}, low_memory=False),
        }

    staging_raw_file = staging_dir / "extracted_raw.csv"
    if staging_raw_file.exists():
        raw_source_df = pd.read_csv(
            staging_raw_file,
            dtype={"InvoiceNo": str, "StockCode": str},
            low_memory=False,
        )
    else:
        raw_source_df = transformed_dfs["fact_sales"]

    conn = get_db_connection(use_database=True)
    try:
        prepare_tables(conn, reset=reset_tables)
        load_dimensions(conn, transformed_dfs)
        load_facts(conn, transformed_dfs["fact_sales"], staging_dir=staging_dir, batch_size=BATCH_SIZE)
        success = validate_etl(conn, raw_source_df)

        elapsed = time.time() - overall_start
        print(f"[SUCCESS] Tahap Loading & Validasi selesai dalam {elapsed:.2f} detik.")
        return success
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        is_success = load_data()
        if not is_success:
            print("[FATAL] ETL dinyatakan GAGAL karena hasil validasi tidak cocok.", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"\n[FATAL] Proses Loading GAGAL: {e}", file=sys.stderr)
        sys.exit(1)
