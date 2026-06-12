# 🎮 Game Price ETL Pipeline

### Advanced Analytics Pipeline — Rekomendasi Harga Top-Up Game Mobile

> Pipeline ETL end-to-end untuk menganalisis pasar top-up game mobile dan memberikan rekomendasi harga berbasis Machine Learning. Data diambil dari 28 sumber publik nyata, diproses, dimuat ke database, dan divisualisasikan melalui BI dashboard interaktif.

---

## 👥 Kontributor

| Nama Lengkap | NIM | Peran |
|---|---|---|
| [Adam Noverian] | 244311033 | Data Engineering |
| [Alfiansyah Wahyu Pratama] | 244311034 | Project Manajer |
| [Ghofur Akbar Munirrullah] | 244311042 | Data Analyst |

---

## 📋 Deskripsi Proyek

Proyek ini membangun **pipeline ETL otomatis** yang mengekstrak data harga top-up, statistik pemain, kompetisi esports, dan konteks pasar dari **28 sumber publik** (GamsGo, SEAGM, Business of Apps, Liquipedia, dll.), mentransformasikan data mentah menjadi dataset bersih dengan feature engineering, memuatnya ke database (SQLite/Aiven PostgreSQL) beserta Parquet warehouse, lalu menghasilkan Business Intelligence dan model ML rekomendasi harga.

### 🎯 Tujuan & Manfaat

| Use Case | Deskripsi |
|---|---|
| **Serving Analisis** | BI dashboard + heatmap korelasi untuk insight harga kompetitif |
| **Serving ML** | Dataset 90 kombinasi (game × region × segmen) untuk model prediksi harga |
| **Keputusan Bisnis** | Kalkulator harga interaktif — input kondisi pasar → output harga optimal |

---

## 🏗️ Arsitektur Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    AIRFLOW DAG ORCHESTRATION                     │
│                                                                  │
│  start ──► extract ──► transform ──► load ────► train_ml ──► notify
│                                  └──► heatmap ─┘                │
└─────────────────────────────────────────────────────────────────┘

EXTRACT (Phase 1)                    TRANSFORM (Phase 2)
├── TopUpExtractor                   ├── transform_topup()       → fact_pricing.csv
│   └── GamsGo, SEAGM, GameBar,     ├── transform_game_stats()  → dim_game.csv
│       LDShop, Codashop, BuffBuff  ├── transform_age()         → dim_age.csv
├── GameStatsExtractor               ├── transform_competition() → fact_competition.csv
│   └── BusinessOfApps, Quantumrun, ├── transform_monthly_revenue() → fact_revenue.csv
│       rec0ded, BitTopup, Udonis    ├── build_ml_dataset()     → ml_features.csv
├── EsportsExtractor                 └── generate_heatmaps()    → 2× PNG
│   └── Liquipedia, EsportsCharts
└── MarketContextExtractor           LOAD (Phase 3)
    └── SQMagazine, TechRT, Accio   ├── SQLite / Aiven PostgreSQL
                                     ├── 6 tabel + audit_log
MACHINE LEARNING (Phase 4)          └── 6× Parquet warehouse
├── GradientBoosting ⭐ (best)
├── XGBoost                          OUTPUT
├── RandomForest                     ├── outputs/charts/ (5 PNG)
└── Ridge (baseline)                 ├── outputs/ml_model.joblib
                                     └── outputs/reports/
```

---

## Pipeline

### 1. Extract (Pengambilan Data)

Data diekstrak dari 28 sumber publik yang dikelompokkan menjadi 4 kategori melalui kelas ekstractor terpisah:

- **TopUpExtractor** — Harga top-up dari platform GamsGo, SEAGM, GameBar, LDShop, Codashop, TOPUPlive, BuffBuff, dan Joytify.
- **GameStatsExtractor** — Statistik pemain, MAU, dan ARPDAU dari Business of Apps, Quantumrun, rec0ded, BitTopup, Udonis, CoopBoardGames, dan IconEra.
- **EsportsExtractor** — Data kompetisi dan prize pool dari Liquipedia, EsportsCharts, dan Wikipedia.
- **MarketContextExtractor** — Konteks pasar global dari SQMagazine, TechRT, Accio, Market.us, dan Statista.

Data mentah disimpan dalam format JSON di direktori `data/raw/` sebelum masuk ke fase transformasi.

### 2. Transform (Pembersihan & Transformasi)

- Membersihkan data mentah JSON menjadi tabel terstruktur (6 file CSV).
- Melakukan feature engineering: value score, price elasticity, competition tier, age price sensitivity.
- Membangun dataset ML dengan 90 kombinasi (6 game × 3 region × 5 segmen spending).
- Menghasilkan 2 heatmap PNG (korelasi fitur & harga per game × region).

### 3. Load (Pemindahan ke Target)

- **Target:** SQLite (development) atau PostgreSQL Cloud di hosting **Aiven** (production).
- Data di-load ke 6 tabel utama + tabel `etl_audit_log` untuk pencatatan integritas.
- Seluruh tabel juga diekspor ke format **Apache Parquet** sebagai data warehouse.
- Setiap load mencatat checksum MD5, jumlah baris, dan timestamp ke audit log.

### 4. Machine Learning

- Melatih 4 model regresi (GradientBoosting, XGBoost, RandomForest, Ridge).
- Model terbaik disimpan sebagai `outputs/ml_model.joblib`.
- Menghasilkan 3 chart evaluasi: feature importance, actual vs predicted, dan perbandingan model.

---

## 📁 Struktur Direktori

```
GamePriceETL/
├── config/
│   └── settings.py              # Semua konfigurasi via env variable
├── dags/
│   └── game_price_etl_dag.py    # Apache Airflow DAG
├── src/
│   ├── extract/
│   │   └── extractor.py         # Phase 1: 4 extractor class
│   ├── transform/
│   │   └── transformer.py       # Phase 2: 6 transform + heatmap
│   ├── load/
│   │   └── loader.py            # Phase 3: DB load + Parquet + audit
│   ├── ml/
│   │   └── model.py             # Phase 4: 4 model ML + inference
│   └── bi/                      # BI dashboard (HTML interaktif)
├── data/
│   ├── raw/                     # JSON mentah dari extractor
│   ├── processed/               # CSV siap analisis (6 file)
│   └── warehouse/               # Parquet + SQLite DB
├── outputs/
│   ├── charts/                  # Heatmap + ML chart PNG
│   └── reports/
├── tests/
│   └── test_pipeline.py         # Unit test semua phase
├── run_pipeline.py              # Runner standalone (tanpa Airflow)
├── requirements.txt
├── .env.example                 # Template environment variable
└── .gitignore
```

---

## 🗄️ Schema Database

```
dim_game              fact_pricing          dim_age
─────────────         ─────────────         ────────────
game_key (PK)         id (PK)               id (PK)
game                  game_key              game_key
publisher             tier_label            age_band
genre                 price_official_usd    pct
mau_millions          price_3rdparty_usd    game_median_age
revenue_2024_usd      value_score           age_price_sensitivity
arpdau_usd            spend_tier
competition_score     price_elasticity      fact_competition
community_health                            ───────────────────
                      fact_revenue_monthly  game_key (PK)
ml_features           ────────────────────  competition_score
───────────           game_key              competition_tier
game_key              year                  prize_pool_2025_usd
region                month_num             peak_viewers
spending_segment      revenue_usd           tournaments_total
...14 features...     seasonal_multiplier
optimal_price_per_...
                      etl_audit_log
                      ─────────────
                      table_name
                      rows_loaded
                      checksum (MD5)
                      status
                      loaded_at
```

---

## 📊 Output yang Dihasilkan

```
data/processed/
  ├── dim_game.csv              (6 rows × 22 cols)
  ├── fact_pricing.csv          (51 rows × 19 cols)
  ├── dim_age.csv               (24 rows × 7 cols)
  ├── fact_competition.csv      (6 rows × 9 cols)
  ├── fact_revenue_monthly.csv  (36 rows × 9 cols)
  └── ml_features.csv           (90 rows × 22 cols)

data/warehouse/
  ├── *.parquet (6 file Parquet)
  └── game_price_etl.db (SQLite)

outputs/charts/
  ├── heatmap_feature_correlation.png   ← Korelasi antar fitur
  ├── heatmap_price_by_game_region.png  ← Harga per game × region
  ├── ml_feature_importance.png
  ├── ml_actual_vs_predicted.png
  └── ml_model_comparison.png
```

---

## 🤖 Hasil Model ML

| Model | R² | MAE (USD) | CV R² (5-fold) |
|---|---|---|---|
| **GradientBoosting** ⭐ | 0.9339 | $0.448 | **0.9768** |
| XGBoost | 0.9272 | $0.436 | 0.9677 |
| RandomForest | ~0.90 | ~$0.55 | ~0.95 |
| Ridge (baseline) | ~0.65 | ~$0.90 | ~0.64 |

Model terbaik (GradientBoosting) digunakan untuk inferensi harga optimal berdasarkan kombinasi genre, kompetisi, statistik pemain, dan region.

### Contoh Inference

```python
import joblib
from src.ml.model import predict_price

model = joblib.load("outputs/ml_model.joblib")
result = predict_price(
    model=model,
    genre="MOBA",
    competition_score=80,
    mau_millions=50,
    arpdau_usd=0.025,
    age_median=22,
    region="SEA",
    spending_segment="dolphin"
)
print(f"Harga optimal: ${result['predicted_price_per_100_usd']}")
print(f"Range: ${result['range_low_usd']} – ${result['range_high_usd']}")
print(f"IDR: Rp{result['predicted_price_per_100_idr']:,}")
```

---

## 🗃️ Sumber Data (28 URL)

### Platform Top-Up

| Platform | URL |
|---|---|
| GamsGo | https://www.gamsgo.com/top-up/mobile-legends |
| SEAGM | https://www.seagm.com/en-us/mobile-legends-diamonds-top-up |
| GameBar | https://www.gamebar.gg/top-up/mobile-legends |
| LDShop | https://www.ldshop.gg/top-up/mobile-legends-bang-bang.html |
| Codashop | https://www.codashop.com/id-en/mobile-legends/ |
| TOPUPlive | https://www.topuplive.com/article/best-discounts-and-offers |
| BuffBuff | https://buffbuff.com/ |
| Joytify | https://www.joytify.com/en-us |

### Analytics & Revenue

| Sumber | URL |
|---|---|
| Business of Apps | https://www.businessofapps.com/data/free-fire-statistics/ |
| Quantumrun – MLBB | https://www.quantumrun.com/consulting/mobile-legends-bang-bang/ |
| rec0ded – MLBB | https://rec0ded88.com/statistics/mobile-legends-bang-bang/ |
| rec0ded – Free Fire | https://rec0ded88.com/statistics/free-fire/ |
| CoopBoardGames | https://coopboardgames.com/online-gaming/mobile-legends-bang-bang/ |
| BitTopup Genshin | https://news.bittopup.com/news/genshin-impact-2025-15.2m-players-0.8b-revenue |
| IconEra – MLBB | https://icon-era.com/blog/mobile-legends-bang-bang-live-player-count-and-statistics.443/ |
| Udonis – Free Fire | https://www.blog.udonis.co/mobile-marketing/mobile-games/free-fire-player-count |
| SQ Magazine | https://sqmagazine.co.uk/mobile-games-statistics/ |
| TechRT | https://techrt.com/mobile-game-spending-statistics/ |
| Accio | https://www.accio.com/business/most-profitable-mobile-games-2025-trend |
| Market.us | https://scoop.market.us/gaming-monetization-statistics/ |
| Quantumrun Mobile | https://www.quantumrun.com/consulting/mobile-game-statistics/ |

### Esports & Kompetisi

| Sumber | URL |
|---|---|
| Liquipedia MLBB | https://liquipedia.net/mobilelegends/Portal:Statistics |
| Liquipedia FF | https://liquipedia.net/freefire/Portal:Statistics |
| Esports Charts | https://escharts.com/games/free-fire |
| Wikipedia MSC 2025 | https://en.wikipedia.org/wiki/MSC_2025 |
| IconEra Stats | https://icon-era.com/statistics/video-game-statistics/ |

### Komunitas & Lainnya

| Sumber | URL |
|---|---|
| Statista Genshin | https://www.statista.com/statistics/1295196/genshin-impact-arpu-country/ |
| iGitems | https://igitems.com/freefire/charts |
| EpicNPC | https://www.epicnpc.com/forums/mobile-legends-bang-bang-mlbb-top-up.3467/ |

---

## ⚙️ Cara Instalasi & Menjalankan

### 1. Clone Repository

```bash
git clone https://github.com/<username>/GamePriceETL.git
cd GamePriceETL
```

### 2. Buat Virtual Environment

```bash
python -m venv venv
source venv/bin/activate        # Linux/Mac
# atau
venv\Scripts\activate           # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Konfigurasi Environment

```bash
cp .env.example .env
# Edit .env — isi DATABASE_URL jika pakai Aiven PostgreSQL
# Jika tidak diisi, pipeline otomatis pakai SQLite lokal
```

### 5. Jalankan Pipeline

```bash
# Full pipeline (Extract → Transform → Load → ML)
python run_pipeline.py

# Jalankan per phase
python run_pipeline.py --phase extract
python run_pipeline.py --phase transform
python run_pipeline.py --phase load
python run_pipeline.py --phase ml
```

### 6. (Opsional) Jalankan dengan Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
airflow users create --username admin --password admin --role Admin \
    --firstname Game --lastname ETL --email admin@example.com
cp dags/game_price_etl_dag.py $AIRFLOW_HOME/dags/

# Terminal 1
airflow scheduler

# Terminal 2
airflow webserver --port 8080

# Trigger manual
airflow dags trigger game_price_etl_pipeline
```

### 7. Jalankan Unit Test

```bash
python -m pytest tests/ -v
python -m pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## 🛠️ Tools & Libraries

| Kategori | Tools |
|---|---|
| **Orchestration** | Apache Airflow 2.x |
| **Web Scraping** | requests, BeautifulSoup4, lxml, fake-useragent, tenacity |
| **Data Processing** | pandas, numpy |
| **Database** | SQLite (dev), Aiven PostgreSQL (prod), SQLAlchemy |
| **Warehouse** | Apache Parquet (via pyarrow) |
| **Machine Learning** | scikit-learn, XGBoost, joblib |
| **Visualisasi** | matplotlib, seaborn, plotly |
| **Logging** | loguru |
| **Testing** | pytest, pytest-cov |
| **Config** | python-dotenv |

---

## 📄 Lisensi

MIT License — bebas digunakan untuk keperluan akademik dan non-komersial.

---

*Game Price ETL Pipeline v2.0 | Data real dari 28 sumber publik*
