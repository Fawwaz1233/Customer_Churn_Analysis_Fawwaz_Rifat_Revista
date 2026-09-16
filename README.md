# Customer Churn Analysis

Proyek analisis *customer churn* yang mencakup proses ETL (Extract, Transform, Load) menggunakan Apache Airflow (DAG) serta validasi kualitas data menggunakan Great Expectations (GX).

## Isi Repository
- `P2M3_fawwaz_rifat_data_raw.csv` — data mentah pelanggan.
- `P2M3_fawwaz_rifat_data_clean.csv` — data pelanggan setelah proses pembersihan.
- `P2M3_fawwaz_rifat_DAG.py` — skrip DAG Airflow untuk orkestrasi pipeline ETL.
- `P2M3_fawwaz_rifat_DAG_graph.png` — visualisasi graf pipeline DAG.
- `P2M3_fawwaz_rifat_GX.ipynb` — notebook validasi data menggunakan Great Expectations.
- `P2M3_fawwaz_rifat_ddl.txt` — skema/DDL basis data yang digunakan.
- `P2M3_fawwaz_rifat_conceptual.txt` — catatan konsep dan penjelasan pipeline.

## Cara Menjalankan
1. Siapkan environment Apache Airflow dan library `great_expectations`, `pandas`.
2. Jalankan `P2M3_fawwaz_rifat_DAG.py` pada Airflow untuk mengeksekusi pipeline ETL.
3. Buka `P2M3_fawwaz_rifat_GX.ipynb` untuk melihat proses validasi kualitas data.
4. Gunakan `P2M3_fawwaz_rifat_ddl.txt` sebagai referensi skema tabel database.

## Author
Fawwaz Rif'at Revista
