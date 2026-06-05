"""
config/settings.py
==================
Konfigurasi terpusat — semua nilai dapat di-override via environment variable.
Tidak ada hardcoding di source code; nilai default hanya sebagai fallback.

Cara penggunaan:
    from config.settings import DB_URL, IDR_PER_USD, RAW_DIR

Environment variables yang didukung:
    DATABASE_URL    : PostgreSQL connection string (Aiven/lokal)
    IDR_PER_USD     : Kurs rupiah terhadap USD
    LOG_LEVEL       : DEBUG | INFO | WARNING | ERROR
    RAW_DATA_DIR    : Path direktori data mentah
    PROCESSED_DIR   : Path direktori data terproses
    WAREHOUSE_DIR   : Path direktori warehouse / Parquet
    OUTPUTS_DIR     : Path direktori output (chart, report)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Root project -----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# ── Database (Kriteria 4: Load ke target DB) --------------------------------
# Gunakan Aiven PostgreSQL bila tersedia, fallback ke SQLite lokal
DB_URL: str = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'data' / 'warehouse' / 'game_price_etl.db'}"
)

# ── Kurs & Konstanta Bisnis -------------------------------------------------
IDR_PER_USD: float = float(os.getenv("IDR_PER_USD", "16250"))
EXTRACT_TIMEOUT: int = int(os.getenv("EXTRACT_TIMEOUT", "15"))  # detik
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))

# ── Direktori Data ----------------------------------------------------------
RAW_DIR       = Path(os.getenv("RAW_DATA_DIR",  str(BASE_DIR / "data" / "raw")))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", str(BASE_DIR / "data" / "processed")))
WAREHOUSE_DIR = Path(os.getenv("WAREHOUSE_DIR", str(BASE_DIR / "data" / "warehouse")))
OUTPUTS_DIR   = Path(os.getenv("OUTPUTS_DIR",   str(BASE_DIR / "outputs")))
CHARTS_DIR    = OUTPUTS_DIR / "charts"
REPORTS_DIR   = OUTPUTS_DIR / "reports"
LOGS_DIR      = BASE_DIR / "logs"

# ── Pastikan direktori ada --------------------------------------------------
for _dir in [RAW_DIR, PROCESSED_DIR, WAREHOUSE_DIR, OUTPUTS_DIR, CHARTS_DIR, REPORTS_DIR, LOGS_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)

# ── Logging ----------------------------------------------------------------
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# ── Daftar game yang diproses (mudah diperluas tanpa ubah kode lain) ---------
GAME_KEYS: list[str] = [
    "mlbb",
    "free_fire",
    "pubg_mobile",
    "genshin_impact",
    "honkai_star_rail",
    "cod_mobile",
]

# ── Mapping region ke indeks harga -----------------------------------------
REGION_PRICE_INDEX: dict[str, float] = {
    "SEA":         0.70,
    "LATAM":       0.75,
    "Global_West": 1.00,
    "East_Asia":   1.20,
    "MENA":        0.85,
}

# ── Spending segment multiplier --------------------------------------------
SEGMENT_MULTIPLIER: dict[str, float] = {
    "whale":   2.8,
    "dolphin": 1.4,
    "minnow":  0.75,
}

# ── Currency key per game (untuk flatten tier data) ------------------------
GAME_CURRENCY_KEY: dict[str, str] = {
    "mlbb":             "diamonds",
    "free_fire":        "diamonds",
    "pubg_mobile":      "uc",
    "genshin_impact":   "crystals",
    "honkai_star_rail": "shards",
    "cod_mobile":       "cp",
}
