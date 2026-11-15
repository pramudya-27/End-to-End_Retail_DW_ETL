"""
Module: transform.py
Deskripsi: Tahap 2 ETL - Mengolah dataset penjualan menjadi struktur Star Schema
terdiri dari 4 tabel dimensi (dim_date, dim_product, dim_customer, dim_country)
dan 1 tabel fakta (fact_sales). Menangani surrogate key, natural key mapping,
serta anggota 'Unknown' untuk transaksi tanpa CustomerID.
"""

import sys
import time
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
import numpy as np

from config import STAGING_DIR


def transform_data(
    input_df: Optional[pd.DataFrame] = None, staging_dir: Optional[Path] = None
) -> Dict[str, pd.DataFrame]:
    """
    Melakukan transformasi data hasil ekstraksi ke Star Schema.

    Langkah-langkah:
    1. Membaca data dari staging jika DataFrame tidak diteruskan langsung.
    2. Membersihkan dan memformat tipe data kolom (tanggal, teks, numerik).
    3. Membangun DimDate (DateKey = YYYYMMDD, FullDate, Day, Month, MonthName, Quarter, Year).
    4. Membangun DimProduct (ProductKey, StockCode unik, Description terstandarisasi).
    5. Membangun DimCustomer (CustomerKey, CustomerID, termasuk anggota khusus 'Unknown' CustomerKey=1).
    6. Membangun DimCountry (CountryKey, CountryName unik).
    7. Melakukan lookup/mapping natural key ke surrogate key pada data transaksi.
    8. Menyiapkan FactSales dengan foreign keys tanpa duplikasi atau kehilangan data.
    9. Menyimpan hasil dimensi dan fakta ke folder staging dalam format CSV.

    Returns:
        Dict[str, pd.DataFrame]: Dictionary berisi DataFrame setiap tabel Star Schema.
    """
    if staging_dir is None:
        staging_dir = STAGING_DIR

    print("\n" + "=" * 65)
    print(">>> TAHAP 2: TRANSFORMATION (transform.py)")
    print("=" * 65)
    start_time = time.time()

    # 1. Mendapatkan data masukan
    if input_df is None:
        staging_raw = staging_dir / "extracted_raw.csv"
        if not staging_raw.exists():
            err_msg = (
                f"[ERROR] Data hasil ekstraksi tidak ditemukan di: {staging_raw}\n"
                f"Silakan jalankan 'python extract.py' terlebih dahulu."
            )
            print(err_msg, file=sys.stderr)
            raise FileNotFoundError(err_msg)
        print(f"[INFO] Membaca data ekstraksi dari staging: {staging_raw} ...")
        df = pd.read_csv(
            staging_raw,
            encoding="utf-8",
            dtype={"InvoiceNo": str, "StockCode": str, "Country": str},
            low_memory=False,
        )
    else:
        df = input_df.copy()

    initial_rows = len(df)
    print(f"[INFO] Total data mentah untuk ditransformasi: {initial_rows:,} baris")

    # 2. Pembersihan dan standarisasi tipe data
    print("[INFO] Standarisasi tipe data teks, numerik, dan tanggal...")
    df["InvoiceNo"] = df["InvoiceNo"].astype(str).str.strip()
    df["StockCode"] = df["StockCode"].astype(str).str.strip()
    df["Description"] = df["Description"].fillna("Unknown").astype(str).str.strip()
    df["Country"] = df["Country"].fillna("Unspecified").astype(str).str.strip()

    # Pastikan numerik valid
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype(int)
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").fillna(0.0).round(2)
    
    # Hitung atau validasi GrossSales = Quantity * UnitPrice
    calculated_gross = (df["Quantity"] * df["UnitPrice"]).round(2)
    df["GrossSales"] = pd.to_numeric(df["GrossSales"], errors="coerce").fillna(calculated_gross).round(2)

    # Parsing tanggal
    dt_series = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df["FullDate"] = dt_series.dt.date
    df["Year"] = dt_series.dt.year.astype(int)
    df["Month"] = dt_series.dt.month.astype(int)
    df["Day"] = dt_series.dt.day.astype(int)
    df["Quarter"] = dt_series.dt.quarter.astype(int)
    df["MonthName"] = dt_series.dt.strftime("%B")
    
    # DateKey format integer YYYYMMDD (contoh: 20101201)
    df["DateKey"] = (df["Year"] * 10000 + df["Month"] * 100 + df["Day"]).astype(int)

    # 3. Membangun DimDate
    print("[INFO] Membuat tabel dimensi: dim_date ...")
    dim_date = (
        df[["DateKey", "FullDate", "Day", "Month", "MonthName", "Quarter", "Year"]]
        .drop_duplicates(subset=["DateKey"])
        .sort_values("DateKey")
        .reset_index(drop=True)
    )
    print(f"       -> Total entri dim_date: {len(dim_date):,} hari unik")

    # 4. Membangun DimProduct
    # Karena satu StockCode di log penjualan bisa memiliki variasi penulisan Description,
    # kita ambil deskripsi terbersih / paling sering muncul (mode) untuk tiap StockCode
    # agar StockCode berkorespondensi 1:1 tanpa menyebabkan duplikasi baris fakta saat join.
    print("[INFO] Membuat tabel dimensi: dim_product ...")
    prod_grouped = (
        df.groupby("StockCode")["Description"]
        .agg(lambda s: s.value_counts().index[0])
        .reset_index()
    )
    dim_product = prod_grouped.sort_values("StockCode").reset_index(drop=True)
    dim_product["ProductKey"] = range(1, len(dim_product) + 1)
    # Susun kolom sesuai skema ERD
    dim_product = dim_product[["ProductKey", "StockCode", "Description"]]
    print(f"       -> Total entri dim_product: {len(dim_product):,} produk unik")

    # 5. Membangun DimCustomer
    # Menangani transaksi dengan CustomerID NULL:
    # Buat satu entri khusus CustomerKey = 1 dengan CustomerID = NULL (Unknown)
    # Semua transaksi valid yang tidak memiliki CustomerID diarahkan ke baris ini.
    print("[INFO] Membuat tabel dimensi: dim_customer ...")
    known_customers = df["CustomerID"].dropna().unique()
    known_customers = sorted([int(c) for c in known_customers])

    customer_records = [{"CustomerKey": 1, "CustomerID": None}]
    for idx, cid in enumerate(known_customers, start=2):
        customer_records.append({"CustomerKey": idx, "CustomerID": cid})
    
    dim_customer = pd.DataFrame(customer_records)
    print(f"       -> Total entri dim_customer: {len(dim_customer):,} pelanggan "
          f"({len(known_customers):,} terdaftar + 1 'Unknown' CustomerKey=1)")

    # 6. Membangun DimCountry
    print("[INFO] Membuat tabel dimensi: dim_country ...")
    unique_countries = sorted(df["Country"].unique())
    dim_country = pd.DataFrame({
        "CountryKey": range(1, len(unique_countries) + 1),
        "CountryName": unique_countries,
    })
    print(f"       -> Total entri dim_country: {len(dim_country):,} negara unik")

    # 7. Mapping Natural Key -> Surrogate Key untuk FactSales
    print("[INFO] Melakukan mapping surrogate key ke FactSales...")
    prod_map = dict(zip(dim_product["StockCode"], dim_product["ProductKey"]))
    cust_map = dict(
        zip(
            dim_customer["CustomerID"].dropna().astype(int),
            dim_customer["CustomerKey"][dim_customer["CustomerID"].notna()],
        )
    )
    country_map = dict(zip(dim_country["CountryName"], dim_country["CountryKey"]))

    # Map ProductKey
    df["ProductKey"] = df["StockCode"].map(prod_map)

    # Map CustomerKey (jika NULL diarahkan ke CustomerKey = 1)
    df["CustomerKey"] = df["CustomerID"].dropna().astype(int).map(cust_map)
    df["CustomerKey"] = df["CustomerKey"].fillna(1).astype(int)

    # Map CountryKey
    df["CountryKey"] = df["Country"].map(country_map)

    # 8. Menyiapkan FactSales
    fact_sales = df[
        [
            "InvoiceNo",
            "DateKey",
            "ProductKey",
            "CustomerKey",
            "CountryKey",
            "Quantity",
            "UnitPrice",
            "GrossSales",
        ]
    ].copy()

    # Validasi Integritas Transformasi
    final_rows = len(fact_sales)
    assert final_rows == initial_rows, (
        f"Integritas baris gagal! Sebelum: {initial_rows}, Sesudah: {final_rows}"
    )

    null_fk_check = fact_sales[["DateKey", "ProductKey", "CustomerKey", "CountryKey"]].isnull().sum()
    if null_fk_check.sum() > 0:
        err_msg = f"[ERROR] Ditemukan foreign key bernilai NULL pada FactSales:\n{null_fk_check}"
        print(err_msg, file=sys.stderr)
        raise ValueError(err_msg)

    print("[INFO] Integritas FactSales terverifikasi:")
    print(f"       Total baris fakta       : {final_rows:,} baris (100% data terjaga)")
    print(f"       Total Quantity          : {fact_sales['Quantity'].sum():,}")
    print(f"       Total GrossSales        : {fact_sales['GrossSales'].sum():,.2f}")
    print(f"       Foreign Key NULL        : 0 (seluruh relasi valid)")

    # 9. Menyimpan hasil transformasi ke staging
    staging_dir.mkdir(parents=True, exist_ok=True)
    print("\n[INFO] Menyimpan hasil transformasi ke folder staging...")

    dim_date.to_csv(staging_dir / "dim_date.csv", index=False, encoding="utf-8")
    dim_product.to_csv(staging_dir / "dim_product.csv", index=False, encoding="utf-8")
    dim_customer.to_csv(staging_dir / "dim_customer.csv", index=False, encoding="utf-8")
    dim_country.to_csv(staging_dir / "dim_country.csv", index=False, encoding="utf-8")
    fact_sales.to_csv(staging_dir / "fact_sales.csv", index=False, encoding="utf-8")

    elapsed_time = time.time() - start_time
    print(f"[SUCCESS] Tahap Transformasi selesai dalam {elapsed_time:.2f} detik.")
    print("=" * 65 + "\n")

    return {
        "dim_date": dim_date,
        "dim_product": dim_product,
        "dim_customer": dim_customer,
        "dim_country": dim_country,
        "fact_sales": fact_sales,
    }


if __name__ == "__main__":
    try:
        transform_data()
    except Exception as e:
        print(f"\n[FATAL] Proses Transformasi GAGAL: {e}", file=sys.stderr)
        sys.exit(1)
