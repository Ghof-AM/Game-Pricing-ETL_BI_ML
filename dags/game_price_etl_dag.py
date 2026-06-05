"""
dags/game_price_etl_dag.py
===========================
Airflow DAG — Game Price ETL Pipeline
Workflow ETL modular dengan Apache Airflow 2.x

Arsitektur DAG:
  extract_task
      ↓
  transform_task
      ↓
  load_task  ──── heatmap_task (paralel)
      ↓
  ml_task
      ↓
  notify_task

Cara menjalankan:
  # 1. Set AIRFLOW_HOME
  export AIRFLOW_HOME=$(pwd)/airflow_home

  # 2. Inisialisasi database Airflow
  airflow db init

  # 3. Letakkan file ini di folder dags/
  # 4. Jalankan scheduler & webserver
  airflow scheduler &
  airflow webserver --port 8080

  # Atau trigger manual via CLI:
  airflow dags trigger game_price_etl_pipeline
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Tambahkan root project ke sys.path agar import src.* berfungsi
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default args DAG ───────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":             "game_etl_team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=3),
}


# ═══════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def task_extract(**context) -> None:
    """Task Extract: ambil data dari semua sumber."""
    from src.extract.extractor import run_extract
    master = run_extract()

    # Push ke XCom agar task berikutnya bisa akses metadata
    context["ti"].xcom_push(
        key="extract_meta",
        value={
            "timestamp":   master.get("_pipeline_meta", {}).get("timestamp"),
            "game_count":  master.get("_pipeline_meta", {}).get("game_count"),
            "source_count":master.get("_pipeline_meta", {}).get("source_count"),
        },
    )


def task_transform(**context) -> None:
    """Task Transform: bersihkan, normalisasi, buat heatmap & ML dataset."""
    from src.transform.transformer import run_transform
    dataframes = run_transform()

    context["ti"].xcom_push(
        key="transform_meta",
        value={
            "tables": list(dataframes.keys()),
            "row_counts": {k: len(v) for k, v in dataframes.items()},
        },
    )


def task_load(**context) -> None:
    """Task Load: muat data ke database & Parquet warehouse."""
    import pandas as pd
    from src.load.loader import run_load
    from config.settings import PROCESSED_DIR

    table_map = {
        "game":    "dim_game",
        "pricing": "fact_pricing",
        "age":     "dim_age",
        "comp":    "fact_competition",
        "revenue": "fact_revenue_monthly",
        "ml":      "ml_features",
    }

    dataframes = {
        alias: pd.read_csv(PROCESSED_DIR / f"{csv_name}.csv")
        for alias, csv_name in table_map.items()
        if (PROCESSED_DIR / f"{csv_name}.csv").exists()
    }

    summary = run_load(dataframes)
    context["ti"].xcom_push(key="load_summary", value=summary)


def task_heatmap(**context) -> None:
    """Task Heatmap (paralel dengan load): buat chart korelasi."""
    import pandas as pd
    from src.transform.transformer import generate_heatmaps
    from config.settings import PROCESSED_DIR

    df_ml   = pd.read_csv(PROCESSED_DIR / "ml_features.csv")
    df_game = pd.read_csv(PROCESSED_DIR / "dim_game.csv")
    generate_heatmaps(df_ml, df_game)


def task_ml(**context) -> None:
    """Task ML: latih model & simpan hasil evaluasi."""
    from src.ml.model import run_ml
    _, results, _ = run_ml()

    # Log hasil ke XCom
    context["ti"].xcom_push(
        key="ml_results",
        value={
            name: {k: v for k, v in res.items()
                   if k not in ["model", "y_pred", "y_test"]}
            for name, res in results.items()
        },
    )


def task_notify(**context) -> None:
    """Task Notify: cetak ringkasan pipeline ke log."""
    ti = context["ti"]

    extract_meta  = ti.xcom_pull(key="extract_meta",  task_ids="extract")  or {}
    transform_meta= ti.xcom_pull(key="transform_meta",task_ids="transform") or {}
    load_summary  = ti.xcom_pull(key="load_summary",  task_ids="load")      or {}
    ml_results    = ti.xcom_pull(key="ml_results",    task_ids="train_ml")  or {}

    best = max(ml_results, key=lambda k: ml_results[k].get("cv_r2", 0)) if ml_results else "N/A"
    best_r2 = ml_results.get(best, {}).get("cv_r2", "N/A")

    print("=" * 60)
    print("PIPELINE SELESAI — Ringkasan")
    print("=" * 60)
    print(f"Extract   : {extract_meta.get('game_count', '?')} games, {extract_meta.get('source_count', '?')} sumber")
    print(f"Transform : {transform_meta.get('tables', '?')}")
    print(f"Load      : {load_summary.get('total_rows', '?')} total baris ke DB")
    print(f"ML        : Best model = {best} | CV R² = {best_r2}")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="game_price_etl_pipeline",
    description=(
        "ETL Pipeline: Game Top-Up Market — Extract dari 28 sumber publik, "
        "Transform ke data warehouse, Load ke PostgreSQL/SQLite, "
        "BI heatmap analysis, ML price recommendation"
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * 1",    # Setiap Senin jam 06:00
    catchup=False,
    tags=["etl", "game", "price", "ml"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
        doc_md="**Extract** — Ambil data dari platform top-up, analytics, dan esports.",
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
        doc_md="**Transform** — Bersihkan, normalisasi, feature engineering, ML dataset.",
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
        doc_md="**Load** — Muat ke PostgreSQL/SQLite + Parquet warehouse.",
    )

    heatmap = PythonOperator(
        task_id="heatmap",
        python_callable=task_heatmap,
        doc_md="**Heatmap** — Generate korelasi heatmap (paralel dengan load).",
    )

    train_ml = PythonOperator(
        task_id="train_ml",
        python_callable=task_ml,
        doc_md="**ML Training** — Latih 4 model, pilih terbaik, simpan joblib.",
    )

    notify = PythonOperator(
        task_id="notify",
        python_callable=task_notify,
        trigger_rule=TriggerRule.ALL_DONE,   # Jalan meskipun ada task gagal
        doc_md="**Notify** — Cetak ringkasan pipeline.",
    )

    # ── Dependency Graph ──────────────────────────────────────────────────
    # start → extract → transform → [load, heatmap] → train_ml → notify
    start >> extract >> transform >> [load, heatmap] >> train_ml >> notify
