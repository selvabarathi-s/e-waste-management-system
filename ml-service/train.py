"""
Train tabular models (RandomForest + optional XGBoost) and write metrics + artifacts to ./models/
"""
from __future__ import annotations

import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split

BASE_DIR = Path(__file__).resolve().parent
MODEL_DIR = BASE_DIR / "models"
MODEL_DIR.mkdir(exist_ok=True)

FEATURE_COLUMNS = ["sales_import_tonnes", "population_millions", "forecast_year"]
TARGET = "disposal_amount_tonnes"


def build_training_frame() -> pd.DataFrame:
    """Synthetic multi-year panel from baseline rows (replace with CSV load for production)."""
    base = pd.DataFrame(
        {
            "sales_import_tonnes": [40, 50, 60, 70, 80, 85, 90],
            "population_millions": [0.11, 0.12, 0.13, 0.14, 0.15, 0.155, 0.16],
            "disposal_amount_tonnes": [15, 20, 25, 28, 30, 32, 34],
        }
    )
    rows = []
    for year in range(2016, 2026):
        for _, r in base.iterrows():
            noise = np.random.RandomState(year).normal(0, 0.02)
            rows.append(
                {
                    "sales_import_tonnes": float(r["sales_import_tonnes"]) * (1 + noise),
                    "population_millions": float(r["population_millions"]) * (1 + noise),
                    "forecast_year": year,
                    "disposal_amount_tonnes": float(r["disposal_amount_tonnes"])
                    * (1 + (year - 2016) * 0.03 + noise),
                }
            )
    return pd.DataFrame(rows)


def train():
    df = build_training_frame()
    X = df[FEATURE_COLUMNS]
    y = df[TARGET]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=120, max_depth=8, random_state=42)
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    rmse = float(np.sqrt(mean_squared_error(y_test, pred)))

    metrics = {
        "tabular": {
            "model": "random_forest",
            "mae": float(mae),
            "rmse": float(rmse),
            "features": FEATURE_COLUMNS,
        }
    }

    xgb_metrics = None
    try:
        import xgboost as xgb  # noqa: F401

        xmodel = xgb.XGBRegressor(
            n_estimators=80, max_depth=4, learning_rate=0.08, random_state=42
        )
        xmodel.fit(X_train, y_train)
        xpred = xmodel.predict(X_test)
        xmae = mean_absolute_error(y_test, xpred)
        xrmse = float(np.sqrt(mean_squared_error(y_test, xpred)))
        if xmae < mae:
            model = xmodel
            metrics["tabular"] = {
                "model": "xgboost",
                "mae": float(xmae),
                "rmse": float(xrmse),
                "features": FEATURE_COLUMNS,
            }
        else:
            xgb_metrics = {"mae": float(xmae), "rmse": float(xrmse), "note": "kept_random_forest"}
    except Exception:
        pass

    if xgb_metrics:
        metrics["tabular_xgb_benchmark"] = xgb_metrics

    with open(MODEL_DIR / "model_tabular.pkl", "wb") as f:
        pickle.dump(model, f)
    with open(MODEL_DIR / "feature_columns.json", "w") as f:
        json.dump(FEATURE_COLUMNS, f)
    with open(MODEL_DIR / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print("Saved model_tabular.pkl, feature_columns.json, metrics.json")
    print(json.dumps(metrics["tabular"], indent=2))


if __name__ == "__main__":
    train()
