"""
Module: extract.py
Deskripsi: Tahap 1 ETL - Mengekstrak data penjualan dari file CSV hasil cleaning,
melakukan validasi awal ketersediaan dan integritas kolom, lalu menyimpannya
ke direktori staging untuk diproses pada tahap Transform.
"""

import sys
import time
from pathlib import Path
import pandas as pd

from config import (
    DATA_FILE_PATH_RESOLVED,
    STAGING_DIR,
    MANDATORY_COLUMNS,
    print_config_summary,
)


def extract_data(source_path: Path = None, staging_dir: Path = None) -> pd.DataFrame:
    """
    Mengekstrak data dari file CSV hasil cleaning.

    Langkah-langkah:
    1. Memvalidasi keberadaan file sumber.
    2. Membaca dataset dengan pandas dan encoding yang sesuai.
    3. Memeriksa keberadaan kolom wajib.
    4. Menampilkan statistik awal dataset (baris, kolom, tipe data, sampel).
    5. Menyimpan data hasil ekstraksi ke folder staging sebagai file sementara.

    Returns:
        pd.DataFrame: Data hasil ekstraksi.
    """
    if source_path is None:
        source_path = DATA_FILE_PATH_RESOLVED
    if staging_dir is None:
        staging_dir = STAGING_DIR

    print("\n" + "=" * 65)
    print(">>> TAHAP 1: EXTRACTION (extract.py)")
    print("=" * 65)
    start_time = time.time()

    # 1. Validasi keberadaan file
    if not source_path.exists():
        err_msg = (
            f"[ERROR] File sumber tidak ditemukan di: {source_path}\n"
            f"Pastikan file 'Online_Retail_Hasil_Cleaning.csv' berada di direktori 'data/' "
            f"atau di root direktori proyek."
        )
        print(err_msg, file=sys.stderr)
        raise FileNotFoundError(err_msg)

    print(f"[INFO] File sumber ditemukan: {source_path}")
    file_size_mb = source_path.stat().st_size / (1024 * 1024)
    print(f"[INFO] Ukuran file: {file_size_mb:.2f} MB")

    # 2. Membaca file dengan pandas (encoding utf-8 dengan fallback latin-1)
    print("[INFO] Membaca file CSV ke dalam DataFrame...")
    try:
        df = pd.read_csv(
            source_path,
            encoding="utf-8",
            dtype={"InvoiceNo": str, "StockCode": str},
            low_memory=False,
        )
    except UnicodeDecodeError:
        print("[WARNING] Gagal membaca dengan UTF-8, mencoba encoding latin-1...")
        df = pd.read_csv(
            source_path,
            encoding="latin-1",
            dtype={"InvoiceNo": str, "StockCode": str},
            low_memory=False,
        )

    # 3. Validasi dimensi data
    total_rows, total_cols = df.shape
    print(f"[INFO] Total baris hasil ekstraksi : {total_rows:,} baris")
    print(f"[INFO] Total kolom hasil ekstraksi : {total_cols} kolom")

    # 4. Validasi kolom wajib
    missing_cols = [col for col in MANDATORY_COLUMNS if col not in df.columns]
    if missing_cols:
        err_msg = f"[ERROR] Kolom wajib berikut tidak ditemukan di dataset: {missing_cols}"
        print(err_msg, file=sys.stderr)
        raise ValueError(err_msg)
    print("[INFO] Semua kolom wajib lengkap terverifikasi:")
    print(f"       {', '.join(MANDATORY_COLUMNS)}")

    # 5. Informasi Dataset dan Ringkasan
    print("\n" + "-" * 40)
    print("RINGKASAN DATASET HASIL EKSTRAKSI:")
    print("-" * 40)
    print(f"Memory Usage   : {df.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB")
    print("\nInformasi Missing Value:")
    null_counts = df.isnull().sum()
    for col, null_c in null_counts.items():
        if null_c > 0:
            print(f"  - {col:<12}: {null_c:,} baris NULL ({null_c / total_rows * 100:.2f}%)")
    if null_counts.sum() == 0:
        print("  (Tidak ada nilai NULL pada seluruh kolom)")

    print("\nSampel 5 Baris Teratas:")
    print(df.head(5)[["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "GrossSales"]])

    # 6. Menyimpan hasil ekstraksi ke folder staging
    staging_dir.mkdir(parents=True, exist_ok=True)
    staging_file = staging_dir / "extracted_raw.csv"

    print(f"\n[INFO] Menyimpan data hasil ekstraksi ke staging: {staging_file} ...")
    df.to_csv(staging_file, index=False, encoding="utf-8")

    elapsed_time = time.time() - start_time
    print(f"[SUCCESS] Tahap Ekstraksi selesai dalam {elapsed_time:.2f} detik.")
    print("=" * 65 + "\n")

    return df


if __name__ == "__main__":
    try:
        extract_data()
    except Exception as e:
        print(f"\n[FATAL] Proses Ekstraksi GAGAL: {e}", file=sys.stderr)
        sys.exit(1)
