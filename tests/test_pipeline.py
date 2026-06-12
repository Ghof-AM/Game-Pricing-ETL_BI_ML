"""
tests/test_pipeline.py
=======================
Unit test untuk semua fase ETL pipeline.

Jalankan:
    cd GamePriceETL
    python -m pytest tests/ -v
    python -m pytest tests/ -v --tb=short   # output ringkas
"""

import sys
from pathlib import Path
import json

import pandas as pd
import numpy as np
import pytest

# Tambah root ke path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ═══════════════════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="session")
def raw_data(tmp_path_factory):
    """Jalankan ekstraksi sekali untuk semua test."""
    from src.extract.extractor import run_extract
    from config.settings import RAW_DIR
    master = run_extract()
    return master


@pytest.fixture(scope="session")
def processed_data(raw_data):
    """Jalankan transform setelah ekstraksi."""
    from src.transform.transformer import run_transform
    return run_transform()


# ═══════════════════════════════════════════════════════════════════════════
# TEST: CONFIG
# ═══════════════════════════════════════════════════════════════════════════

class TestConfig:
    def test_settings_importable(self):
        from config.settings import (
            DB_URL, IDR_PER_USD, GAME_KEYS, REGION_PRICE_INDEX, SEGMENT_MULTIPLIER
        )
        assert isinstance(DB_URL, str) and len(DB_URL) > 0
        assert IDR_PER_USD > 0
        assert len(GAME_KEYS) == 6
        assert "SEA" in REGION_PRICE_INDEX
        assert "whale" in SEGMENT_MULTIPLIER

    def test_no_hardcoded_credentials(self):
        """Pastikan tidak ada password/key hardcoded di settings."""
        from config import settings
        import inspect
        src = inspect.getsource(settings)
        forbidden = ["password=", "secret=", "apikey=", "api_key="]
        for term in forbidden:
            assert term not in src.lower(), f"Possible hardcoded credential: '{term}'"

    def test_directories_created(self):
        from config.settings import RAW_DIR, PROCESSED_DIR, WAREHOUSE_DIR, OUTPUTS_DIR
        for d in [RAW_DIR, PROCESSED_DIR, WAREHOUSE_DIR, OUTPUTS_DIR]:
            assert Path(d).exists(), f"Directory not created: {d}"


# ═══════════════════════════════════════════════════════════════════════════
# TEST: EXTRACT
# ═══════════════════════════════════════════════════════════════════════════

class TestExtract:
    def test_raw_data_keys(self, raw_data):
        """Semua seksi utama harus ada di master raw."""
        for key in ["topup", "stats", "esports", "market", "_pipeline_meta"]:
            assert key in raw_data, f"Key '{key}' tidak ada di raw_data"

    def test_all_games_present(self, raw_data):
        from config.settings import GAME_KEYS
        for gk in GAME_KEYS:
            assert gk in raw_data["topup"], f"Game '{gk}' tidak ada di topup"
            assert gk in raw_data["stats"], f"Game '{gk}' tidak ada di stats"

    def test_tiers_not_empty(self, raw_data):
        """Setiap game harus punya minimal 3 tier harga."""
        for gk, data in raw_data["topup"].items():
            if gk.startswith("_"):
                continue
            tiers = data.get("tiers", [])
            assert len(tiers) >= 3, f"Game '{gk}' hanya punya {len(tiers)} tier"

    def test_raw_json_saved(self):
        from config.settings import RAW_DIR
        raw_file = RAW_DIR / "master_raw.json"
        assert raw_file.exists(), "master_raw.json tidak ditemukan"
        with open(raw_file) as f:
            data = json.load(f)
        assert "topup" in data

    def test_sources_registry_complete(self):
        from src.extract.extractor import DATA_SOURCES
        assert "top_up_platforms" in DATA_SOURCES
        assert "analytics" in DATA_SOURCES
        assert "esports" in DATA_SOURCES
        total_sources = sum(len(v) for v in DATA_SOURCES.values())
        assert total_sources >= 20, f"Sumber terlalu sedikit: {total_sources}"

    def test_pipeline_meta(self, raw_data):
        meta = raw_data.get("_pipeline_meta", {})
        assert meta.get("game_count") == 6
        assert meta.get("source_count") >= 20


# ═══════════════════════════════════════════════════════════════════════════
# TEST: TRANSFORM
# ═══════════════════════════════════════════════════════════════════════════

class TestTransform:
    def test_all_tables_returned(self, processed_data):
        expected = ["pricing", "game", "age", "comp", "revenue", "ml"]
        for key in expected:
            assert key in processed_data, f"Tabel '{key}' tidak ada"

    def test_fact_pricing_schema(self, processed_data):
        df = processed_data["pricing"]
        required_cols = [
            "game_key", "tier_label", "price_official_usd", "price_official_idr",
            "price_3rdparty_usd", "value_score", "spend_tier", "price_elasticity",
        ]
        for col in required_cols:
            assert col in df.columns, f"Kolom '{col}' tidak ada di fact_pricing"

    def test_no_negative_prices(self, processed_data):
        df = processed_data["pricing"]
        assert (df["price_official_usd"] > 0).all(), "Ada harga negatif/nol"

    def test_no_duplicates_dim_game(self, processed_data):
        df = processed_data["game"]
        assert df["game_key"].nunique() == len(df), "Ada duplikat di dim_game"

    def test_idr_conversion_correct(self, processed_data):
        from config.settings import IDR_PER_USD
        df = processed_data["pricing"]
        # Toleransi ±1 IDR karena intermediate rounding float
        computed = (df["price_official_usd"] * IDR_PER_USD).round().astype(int)
        diff = (computed - df["price_official_idr"]).abs()
        assert (diff <= 1).all(), f"Konversi IDR meleset >1: {diff[diff>1]}"

    def test_age_pct_sum_per_game(self, processed_data):
        """Total persentase usia per game harus ~100%."""
        df = processed_data["age"]
        totals = df.groupby("game_key")["pct"].sum()
        for gk, total in totals.items():
            assert 95 <= total <= 105, f"Distribusi usia {gk} = {total}% (bukan ~100%)"

    def test_community_health_range(self, processed_data):
        df = processed_data["game"]
        assert df["community_health_score"].between(0, 100).all(), \
            "community_health_score di luar range 0–100"

    def test_ml_dataset_dimensions(self, processed_data):
        df = processed_data["ml"]
        # 6 games × 5 regions × 3 segments = 90 baris
        assert len(df) == 90, f"ML dataset harus 90 baris, dapat {len(df)}"
        assert df["optimal_price_per_100_usd"].gt(0).all(), "Ada target price <= 0"

    def test_csv_files_saved(self):
        from config.settings import PROCESSED_DIR
        files = ["dim_game.csv", "fact_pricing.csv", "dim_age.csv",
                 "fact_competition.csv", "fact_revenue_monthly.csv", "ml_features.csv"]
        for f in files:
            assert (PROCESSED_DIR / f).exists(), f"{f} tidak ditemukan"

    def test_heatmap_generated(self):
        from config.settings import CHARTS_DIR
        assert (CHARTS_DIR / "heatmap_feature_correlation.png").exists(), \
            "Heatmap korelasi tidak ditemukan"
        assert (CHARTS_DIR / "heatmap_price_by_game_region.png").exists(), \
            "Heatmap price tidak ditemukan"


# ═══════════════════════════════════════════════════════════════════════════
# TEST: LOAD
# ═══════════════════════════════════════════════════════════════════════════

class TestLoad:
    def test_load_to_db(self, processed_data):
        from src.load.loader import run_load
        summary = run_load(processed_data)
        assert summary["total_rows"] > 0

    def test_db_tables_populated(self):
        from config.settings import DB_URL
        from sqlalchemy import create_engine, text, inspect
        engine = create_engine(DB_URL, connect_args={"check_same_thread": False}
                               if DB_URL.startswith("sqlite") else {})
        insp = inspect(engine)
        existing_tables = insp.get_table_names()
        expected = ["dim_game", "fact_pricing", "dim_age",
                    "fact_competition", "fact_revenue_monthly", "ml_features"]
        for tbl in expected:
            assert tbl in existing_tables, f"Tabel '{tbl}' tidak ada di DB"

        with engine.connect() as conn:
            for tbl in expected:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
                assert count > 0, f"Tabel '{tbl}' kosong"

    def test_parquet_files_saved(self):
        from config.settings import WAREHOUSE_DIR
        parquet_files = list(WAREHOUSE_DIR.glob("*.parquet"))
        assert len(parquet_files) >= 6, f"Hanya {len(parquet_files)} parquet files"

    def test_audit_log_written(self):
        from config.settings import DB_URL
        from sqlalchemy import create_engine, text
        engine = create_engine(DB_URL, connect_args={"check_same_thread": False}
                               if DB_URL.startswith("sqlite") else {})
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM etl_audit_log")).scalar()
        assert count > 0, "etl_audit_log kosong"


# ═══════════════════════════════════════════════════════════════════════════
# TEST: ML
# ═══════════════════════════════════════════════════════════════════════════

class TestML:
    def test_ml_training_runs(self):
        from src.ml.model import run_ml
        model, results, feat_imp = run_ml()
        assert model is not None
        assert len(results) == 4

    def test_best_model_accuracy(self):
        from src.ml.model import run_ml
        _, results, _ = run_ml()
        best_r2 = max(r["r2"] for r in results.values())
        assert best_r2 >= 0.85, f"R² terlalu rendah: {best_r2:.4f}"

    def test_feature_importance_completeness(self):
        from src.ml.model import run_ml, FEATURE_COLS
        _, _, df_imp = run_ml()
        assert len(df_imp) == len(FEATURE_COLS)
        assert abs(df_imp["importance_pct"].sum() - 100) < 1.0  # Toleransi rounding 1%

    def test_predict_price_output(self):
        import joblib
        from src.ml.model import predict_price
        from config.settings import OUTPUTS_DIR
        model_path = OUTPUTS_DIR / "ml_model.joblib"
        if not model_path.exists():
            pytest.skip("Model belum ditraining")
        model = joblib.load(model_path)
        result = predict_price(model, "MOBA", 80, 50, 0.025, 22, "SEA", "dolphin")
        assert result["predicted_price_per_100_usd"] > 0
        assert result["range_low_usd"] < result["predicted_price_per_100_usd"]
        assert result["range_high_usd"] > result["predicted_price_per_100_usd"]
        assert result["predicted_price_per_100_idr"] > 0

    def test_model_saved(self):
        from config.settings import OUTPUTS_DIR
        assert (OUTPUTS_DIR / "ml_model.joblib").exists(), "ml_model.joblib tidak ditemukan"
