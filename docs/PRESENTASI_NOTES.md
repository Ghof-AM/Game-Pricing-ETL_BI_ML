# 🎤 Naskah Presentasi ETL — 10 Menit
## Game Price ETL Pipeline

---

## MENIT 0–1 | PEMBUKAAN & JUDUL PROYEK

**Yang disampaikan:**
> "Proyek ini berjudul **'Game Price ETL Pipeline — Advanced Analytics untuk Rekomendasi Harga Top-Up Game Mobile'**.
> Tujuannya: membangun pipeline ETL otomatis yang mengambil data real dari 28 sumber publik,
> memprosesnya menjadi dataset bersih, memuatnya ke database, lalu menghasilkan
> Business Intelligence dan model Machine Learning yang bisa merekomendasikan harga
> top-up kompetitif untuk pasar game mobile."

**Tampilkan:** Slide arsitektur pipeline (diagram ETL → BI → ML)

---

## MENIT 1–3 | EXTRACT — Pengambilan Data

**Yang disampaikan:**
> "Phase Extract mengambil data dari 4 kategori sumber:
> - **Platform top-up** seperti GamsGo, SEAGM, GameBar, Codashop — untuk harga resmi vs 3rd-party
> - **Analytics** seperti Business of Apps, Quantumrun, BitTopup — untuk MAU, revenue, ARPDAU
> - **Esports** seperti Liquipedia, Esports Charts — untuk tingkat kompetisi dan prize pool
> - **Konteks pasar global** dari SQ Magazine, TechRT, Accio"
>
> "Kami menggunakan HTTP scraping dengan BeautifulSoup, dilengkapi retry otomatis via Tenacity
> dan rotasi User-Agent agar tidak diblokir. Jika sumber tidak tersedia, extractor
> fallback ke dataset terverifikasi tanpa menghentikan pipeline."

**Demo:** Jalankan `python run_pipeline.py --phase extract`
Tunjukkan: log "✓ topup extracted OK", "✓ stats extracted OK"
Buka: `data/raw/master_raw.json`

---

## MENIT 3–5 | TRANSFORM — Pembersihan & Transformasi

**Yang disampaikan:**
> "Phase Transform melakukan 6 operasi utama:
> 1. Flatten tier harga ke tabel relasional dengan derived columns: price_per_100_usd, value_score, price_elasticity
> 2. Normalisasi statistik game: community_health_score (gabungan MAU, DAU ratio, downloads)
> 3. Deduplikasi berdasarkan primary key — duplikat di-log dan dihapus
> 4. Validasi: tidak ada harga negatif, pct usia harus ~100% per game
> 5. Feature engineering untuk ML: 14 fitur × 90 kombinasi (6 game × 5 region × 3 segmen)
> 6. Heatmap korelasi — menggunakan Seaborn — ini output wajib sesuai instruksi proyek"

**Demo:** `python run_pipeline.py --phase transform`
Tunjukkan: kedua heatmap PNG di `outputs/charts/`
Buka: `data/processed/ml_features.csv` — jelaskan 14 kolom fitur

---

## MENIT 5–7 | LOAD — Pemindahan ke Target

**Yang disampaikan:**
> "Phase Load memuat data ke 2 target:
> - **Database**: SQLite lokal (development) atau Aiven PostgreSQL (production).
>   Koneksi dikonfigurasi via environment variable DATABASE_URL — tidak ada credential hardcoded.
>   Pipeline membuat schema otomatis dan mencatat setiap operasi ke tabel etl_audit_log dengan checksum MD5.
> - **Parquet Warehouse**: 6 file format kolumnar untuk analytics masa depan"
>
> "Setelah load, ada validasi integritas: hitung baris per tabel, cek foreign key — 
> fact_pricing.game_key harus ada di dim_game."

**Demo:** Buka `data/warehouse/game_price_etl.db` dengan DB Browser for SQLite
Tunjukkan: 7 tabel, jumlah baris, tabel etl_audit_log dengan checksum

---

## MENIT 7–9 | BI + ML — Output Analitik

**Yang disampaikan — BI:**
> "Untuk Business Intelligence, kami menghasilkan:
> - 2 heatmap Python (Seaborn) sesuai instruksi proyek
> - Dashboard HTML interaktif dengan 6 halaman: ringkasan pasar, profil game, analisis harga, demografi pemain, kalkulator harga AI, dan kamus istilah
> - Untuk koneksi ke Power BI atau Looker: gunakan file Parquet di data/warehouse/ atau sambungkan langsung ke Aiven PostgreSQL"

**Yang disampaikan — ML:**
> "Model ML membandingkan 4 algoritma. Pemenangnya Gradient Boosting dengan CV R²=0.977.
> 14 fitur input, target: optimal_price_per_100_usd.
> Fitur terpenting: spending segment (28%), ARPDAU (22%), competition score (18%)."

**Demo inference langsung:**
```python
import joblib
from src.ml.model import predict_price
model = joblib.load("outputs/ml_model.joblib")
print(predict_price(model, "MOBA", 80, 50, 0.025, 22, "SEA", "dolphin"))
```

---

## MENIT 9–10 | WORKFLOW & PENUTUP

**Yang disampaikan:**
> "Seluruh pipeline diorkestrasikan dengan Apache Airflow. DAG-nya modular:
> extract → transform → [load, heatmap paralel] → ML → notify.
> Bisa dijadwalkan tiap Senin jam 6 pagi, atau di-trigger manual.
> 
> Untuk koneksi Aiven: cukup ganti DATABASE_URL di file .env — tidak perlu ubah kode.
> Semua konfigurasi zero hardcoding menggunakan environment variable."
>
> "Project ini sudah di-upload ke GitHub dengan README lengkap, termasuk instruksi setup,
> daftar 28 sumber data, schema database, dan cara penggunaan model ML."

**Tunjukkan:** Airflow DAG graph (screenshot atau live jika sudah setup)

---

## PERTANYAAN YANG MUNGKIN DITANYA

**Q: Kenapa tidak pakai Power BI langsung?**
A: Power BI membutuhkan lisensi dan koneksi gateway. Kami menyiapkan data di Parquet dan Aiven PostgreSQL yang bisa langsung diconnect ke Power BI Desktop. Heatmap Python kami menggunakan Seaborn sesuai instruksi "analisis heatmap untuk data anda (Python)".

**Q: Bagaimana menambah game baru?**
A: Cukup tambah entry di `VERIFIED_TOPUP` dan `VERIFIED_GAME_STATS` di `extractor.py`, tambah key di `GAME_KEYS` di `settings.py`, lalu jalankan ulang pipeline. Tidak perlu ubah kode di tempat lain.

**Q: Mengapa SQLite bukan langsung Aiven?**
A: SQLite adalah fallback development. Di production, set `DATABASE_URL=postgresql://...` di `.env` — kode loader otomatis detect dan pakai psycopg2 untuk PostgreSQL. Tidak ada perubahan kode sama sekali.

**Q: Akurasi model ML sudah cukup?**
A: CV R²=0.977 sangat baik untuk dataset 90 baris. Model divalidasi dengan 5-fold cross-validation untuk memastikan tidak overfitting. MAE=$0.45 artinya prediksi meleset rata-rata $0.45 per 100 unit currency — sangat acceptable untuk use case ini.

**Q: Apa bedanya Parquet dan CSV?**
A: CSV untuk analisis manual dan sharing. Parquet format kolumnar — jauh lebih cepat untuk query analytics di tools seperti Power BI, Tableau, atau Apache Spark. Ukurannya juga 5-10× lebih kecil.
