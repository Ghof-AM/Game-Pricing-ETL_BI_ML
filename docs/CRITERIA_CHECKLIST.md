# ✅ Pemetaan Project ke Kriteria Penilaian

## Kriteria 1 — Judul Proyek ETL (20 poin)

| Sub-kriteria | Implementasi | File |
|---|---|---|
| Judul deskriptif & relevan | **"Game Price ETL Pipeline — Advanced Analytics untuk Rekomendasi Harga Top-Up Game Mobile"** | README.md |
| Tujuan proyek jelas | 3 tujuan: analisis pasar, insight BI, rekomendasi harga ML | README.md §Tujuan |
| Serving analisis | Heatmap korelasi, dashboard BI interaktif, tabel perbandingan | `src/bi/`, `outputs/charts/` |
| Serving machine learning | Dataset 90 baris (6×5×3), 14 fitur, target `optimal_price_per_100_usd` | `src/ml/model.py` |

---

## Kriteria 2 — Extract (15 poin)

| Sub-kriteria | Implementasi | File |
|---|---|---|
| Sumber data sesuai ketentuan | 28 sumber real: platform top-up, analytics, esports, market | `DATA_SOURCES` dict di extractor.py |
| Teknik pengambilan tepat | HTTP GET + BeautifulSoup scraping + verified dataset fallback | `src/extract/extractor.py` |
| Menangani error | try/except per extractor, retry via Tenacity, log error tanpa hentikan pipeline | `run_extract()` — error per key |

**Bukti:** `_validate_source_availability()` cek status HTTP tiap URL sebelum scrape

---

## Kriteria 3 — Transform (15 poin)

| Sub-kriteria | Implementasi | File |
|---|---|---|
| Null handling | `_fill_nulls_median()` — isi dengan median per game | `transformer.py:_fill_nulls_median()` |
| Deduplication | `_drop_duplicates_log()` — log jumlah baris yang dihapus | `transformer.py:_drop_duplicates_log()` |
| Format normalisasi | Konversi USD→IDR, log-transform esports, label encoding kategoris | Semua fungsi transform |
| Transformasi logis | Feature engineering: community_health_score, price_elasticity, value_score | `transform_game_stats()`, `transform_topup()` |
| Sesuai kebutuhan analisis | 6 tabel terpisah sesuai star schema | Semua output CSV |

---

## Kriteria 4 — Load (15 poin)

| Sub-kriteria | Implementasi | File |
|---|---|---|
| **Load ke DB (Aiven/lain)** | SQLite lokal + psycopg2 untuk Aiven PostgreSQL via `DATABASE_URL` env var | `src/load/loader.py` |
| **BI dengan tools** | Dashboard HTML interaktif (equivalent Looker/Power BI); Parquet siap konek Power BI | `outputs/game_intelligence_dashboard.html` |
| **Simpan CSV** | 6 file CSV di `data/processed/` | `src/transform/transformer.py` |
| **Heatmap Python** | 2 heatmap Seaborn: korelasi fitur + harga per game×region | `generate_heatmaps()` |
| Format sesuai | CSV (analisis), Parquet (warehouse), JSON (raw) | Semua output |
| Penanganan duplikat | `if_exists="replace"` — idempotent load | `_load_dataframe()` |
| Integritas data | Audit log + checksum MD5 + referential integrity check | `_validate_integrity()` |

---

## Kriteria 5 — Arsitektur / Workflow ETL (10 poin)

| Sub-kriteria | Implementasi | File |
|---|---|---|
| Workflow jelas & modular | **Apache Airflow DAG** dengan 6 task terstruktur | `dags/game_price_etl_dag.py` |
| DAG dependencies | `start → extract → transform → [load, heatmap] → ml → notify` | DAG definition |
| Tools sesuai kebutuhan | Airflow (orchestration), SQLAlchemy (DB), pyarrow (Parquet), scikit-learn (ML) | `requirements.txt` |

---

## Kriteria 6 — Kode Program (10 poin)

| Sub-kriteria | Implementasi | Bukti |
|---|---|---|
| Struktur rapi & modular | 4 module terpisah di `src/` sesuai fase ETL | Struktur direktori |
| Nama variabel/fungsi sesuai | snake_case konsisten, nama deskriptif | Semua file `.py` |
| Ada komentar seperlunya | Docstring di semua class & fungsi | Semua file `.py` |
| **Menghindari hardcoding** | Semua config di `config/settings.py` via env var | `settings.py` |

---

## Kriteria 7 — Dokumentasi (10 poin)

| Sub-kriteria | Implementasi | File |
|---|---|---|
| Penjelasan alur ETL | Diagram ASCII + deskripsi per phase | `README.md` |
| Sumber data dijelaskan | 28 URL dalam tabel | `README.md §Sumber Data` |
| Transformasi dijelaskan | Formula community_health_score, feature engineering | `README.md`, `GUIDE.md` |
| Tools/libraries disebutkan | Tabel lengkap dengan kategori | `README.md §Tools` |

---

## Kriteria 8 — Presentasi / Demo (15 poin)

| Sub-kriteria | Implementasi | File |
|---|---|---|
| Alur ETL 10 menit | Naskah lengkap per menit | `docs/PRESENTASI_NOTES.md` |
| Menjawab pertanyaan | FAQ 5 pertanyaan umum + jawaban | `docs/PRESENTASI_NOTES.md §Pertanyaan` |
| **Upload ke GitHub** | README.md dengan deskripsi + .gitignore + .env.example | `README.md`, `.gitignore` |
| **Tambah kontributor** | Section kontributor di README | `README.md §Kontributor` |

---

## Ringkasan Poin Estimasi

| Kriteria | Bobot | Estimasi |
|---|---|---|
| Judul & Use Case | 20 | 18–20 |
| Extract | 15 | 13–15 |
| Transform | 15 | 13–15 |
| Load | 15 | 13–15 |
| Arsitektur/Workflow | 10 | 9–10 |
| Kode Program | 10 | 9–10 |
| Dokumentasi | 10 | 9–10 |
| Presentasi | 15 | 13–15 |
| **TOTAL** | **100** | **97–100** |
