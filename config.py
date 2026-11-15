"""
Module: config.py
Deskripsi: Modul konfigurasi terpusat untuk proses ETL Online Retail Data Warehouse.
Mendukung database MySQL 8.0 dan PostgreSQL / Supabase Cloud secara fleksibel.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Direktori utama proyek
BASE_DIR = Path(__file__).resolve().parent

# Memuat file .env jika ada
ENV_PATH = BASE_DIR / ".env"
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
else:
    load_dotenv()

# Konfigurasi Koneksi Database
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "online_retail_dw")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require" if "supabase" in DB_HOST.lower() else "prefer")

# Deteksi otomatis jenis DBMS: 'postgres' (Supabase) atau 'mysql'
_explicit_type = os.getenv("DB_TYPE", "").lower()
if _explicit_type in ("postgres", "postgresql", "supabase"):
    DB_TYPE = "postgres"
elif _explicit_type == "mysql":
    DB_TYPE = "mysql"
else:
    # Auto-detect jika tidak diset eksplisit
    if DB_PORT == 5432 or "supabase" in DB_HOST.lower() or "postgres" in DB_HOST.lower():
        DB_TYPE = "postgres"
    else:
        DB_TYPE = "mysql"

# Konfigurasi Path File Input
DATA_FILE_PATH = os.getenv("DATA_FILE_PATH", "data/Online_Retail_Hasil_Cleaning.csv")
raw_data_path = BASE_DIR / DATA_FILE_PATH
if not raw_data_path.exists():
    fallback_path = BASE_DIR / "Online_Retail_Hasil_Cleaning.csv"
    if fallback_path.exists():
        raw_data_path = fallback_path

DATA_FILE_PATH_RESOLVED = raw_data_path

# Konfigurasi Direktori Staging Sementara
STAGING_DIR_NAME = os.getenv("STAGING_DIR", "staging")
STAGING_DIR = BASE_DIR / STAGING_DIR_NAME

# Parameter Eksekusi
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20000"))
RESET_TABLES = os.getenv("RESET_TABLES", "true").lower() in ("true", "1", "yes")

# Kolom Wajib pada Dataset Hasil Cleaning
MANDATORY_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
    "GrossSales",
    "Year",
    "Month",
    "Day",
    "Quarter",
]


def get_db_connection(use_database: bool = True):
    """
    Membuat koneksi native ke database:
    - Jika DB_TYPE == 'postgres' (Supabase): menggunakan psycopg2 dengan SSL
    - Jika DB_TYPE == 'mysql': menggunakan PyMySQL
    """
    if DB_TYPE == "postgres":
        import psycopg2
        import psycopg2.extras
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            sslmode=DB_SSLMODE,
        )
    else:
        import pymysql
        import pymysql.cursors
        return pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME if use_database else None,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )


def get_db_engine(use_database: bool = True):
    """
    Membuat SQLAlchemy engine sesuai DBMS aktif.
    """
    if DB_TYPE == "postgres":
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME if use_database else None,
            query={"sslmode": DB_SSLMODE},
        )
    else:
        url = URL.create(
            drivername="mysql+pymysql",
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME if use_database else None,
            query={"charset": "utf8mb4"},
        )
    return create_engine(url, pool_pre_ping=True)


def print_config_summary():
    """Menampilkan ringkasan konfigurasi (dengan masking password)."""
    masked_pw = "*" * len(DB_PASSWORD) if DB_PASSWORD else "(None/Empty)"
    dbms_display = "Supabase Cloud (PostgreSQL)" if DB_TYPE == "postgres" else "MySQL 8.0"
    print("=" * 60)
    print("KONFIGURASI ETL ONLINE RETAIL DATA WAREHOUSE")
    print("=" * 60)
    print(f"DBMS        : {dbms_display}")
    print(f"Host        : {DB_HOST}:{DB_PORT}")
    print(f"User        : {DB_USER}")
    print(f"Password    : {masked_pw}")
    print(f"Database    : {DB_NAME}")
    if DB_TYPE == "postgres":
        print(f"SSL Mode    : {DB_SSLMODE}")
    print(f"File Sumber : {DATA_FILE_PATH_RESOLVED}")
    print(f"Dir Staging : {STAGING_DIR}")
    print(f"Batch Size  : {BATCH_SIZE:,}")
    print(f"Reset Tables: {RESET_TABLES}")
    print("=" * 60)


if __name__ == "__main__":
    print_config_summary()
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT version();" if DB_TYPE == "postgres" else "SELECT VERSION() AS ver;")
        res = cur.fetchone()
        print(f"Status Koneksi: Berhasil terhubung ke database! Versi: {res}")
        conn.close()
    except Exception as e:
        print(f"Status Koneksi: GAGAL terhubung ke database. Pesan: {e}")
