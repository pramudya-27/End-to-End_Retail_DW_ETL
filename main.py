"""
Module: main.py
Deskripsi: Orkestrator Pipeline ETL End-to-End untuk Online Retail Data Warehouse.
Menjalankan tahap Extract, Transform, dan Load secara sekuensial dan otomatis
hanya dengan satu perintah: `python main.py`.
"""

import sys
import time

from config import print_config_summary
from extract import extract_data
from transform import transform_data
from load import load_data


def run_pipeline() -> bool:
    """
    Menjalankan keseluruhan alur kerja ETL:
    Extract -> Transform -> Load & Validate.
    """
    total_start_time = time.time()

    print("\n" + "=" * 70)
    print("  ONLINE RETAIL DATA WAREHOUSE - AUTOMATED ETL PIPELINE")
    print("  Production-Grade Data Engineering Project")
    print("=" * 70)

    # 1. Tampilkan ringkasan konfigurasi
    print_config_summary()

    try:
        # 2. Tahap Ekstraksi
        extracted_df = extract_data()

        # 3. Tahap Transformasi
        transformed_dfs = transform_data(input_df=extracted_df)

        # 4. Tahap Loading & Validasi
        success = load_data(transformed_dfs=transformed_dfs)

        total_elapsed = time.time() - total_start_time
        print("\n" + "=" * 70)
        if success:
            print(f" PIPELINE ETL SELESAI DENGAN SUKSES DALAM {total_elapsed:.2f} DETIK!")
            print(" Seluruh data berhasil dimuat ke database MySQL (online_retail_dw).")
        else:
            print(f" PIPELINE ETL BERAKHIR DENGAN PERINGATAN/KEGAGALAN VALIDASI.")
        print("=" * 70 + "\n")
        return success

    except Exception as e:
        print(f"\n[FATAL ERROR] Pipeline ETL terhenti akibat error: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    is_ok = run_pipeline()
    if not is_ok:
        sys.exit(1)
