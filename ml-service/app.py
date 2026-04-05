from __future__ import annotations

import json
import pickle
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request
from sklearn.cluster import DBSCAN, KMeans
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.arima.model import ARIMA

from preprocess import clean_data

BASE_DIR = Path(__file__).resolve().parent
MODEL_DIR = BASE_DIR / "models"

app = Flask(__name__)

model_tabular = None
legacy_model = None
feature_columns: list[str] | None = None
metrics_cached: dict = {}


def load_models():
    global model_tabular, legacy_model, feature_columns, metrics_cached
    tab_path = MODEL_DIR / "model_tabular.pkl"
    feat_path = MODEL_DIR / "feature_columns.json"
    met_path = MODEL_DIR / "metrics.json"
    legacy_path = BASE_DIR / "model.pkl"

    if feat_path.exists():
        with open(feat_path) as f:
            feature_columns = json.load(f)
    if tab_path.exists():
        with open(tab_path, "rb") as f:
            model_tabular = pickle.load(f)
    if legacy_path.exists():
        with open(legacy_path, "rb") as f:
            legacy_model = pickle.load(f)
    if met_path.exists():
        with open(met_path) as f:
            metrics_cached = json.load(f)


load_models()


@app.route("/health", methods=["GET"])
def health():
    return jsonify(
        {
            "ok": True,
            "tabular_loaded": model_tabular is not None,
            "legacy_loaded": legacy_model is not None,
            "feature_columns": feature_columns,
            "metrics": metrics_cached,
        }
    )


@app.route("/predict", methods=["POST"])
def predict_legacy():
    """Backward-compatible: body = list[{sales_import_tonnes, population_millions}]"""
    if not legacy_model and not model_tabular:
        return jsonify({"error": "Model not found. Run train.py or train_mock_model.py."}), 500
    try:
        data = request.json
        df = pd.DataFrame(data)
        if model_tabular and feature_columns:
            fy = float(request.args.get("forecast_year", 2026))
            for c in feature_columns:
                if c not in df.columns and c == "forecast_year":
                    df[c] = fy
        df_clean = clean_data(df)
        if model_tabular and feature_columns:
            X = df_clean[feature_columns]
            predictions = model_tabular.predict(X)
        else:
            features = df_clean[["sales_import_tonnes", "population_millions"]]
            predictions = legacy_model.predict(features)
        return jsonify({"predictions": predictions.tolist()})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/forecast/tabular", methods=["POST"])
def forecast_tabular():
    if not model_tabular or not feature_columns:
        if legacy_model:
            return predict_legacy()
        return jsonify({"error": "Tabular model not trained. Run ml-service/train.py"}), 500
    try:
        body = request.json or {}
        sales = float(body["sales_import_tonnes"])
        population = float(body["population_millions"])
        forecast_year = float(body["forecast_year"])
        row = pd.DataFrame(
            [
                {
                    "sales_import_tonnes": sales,
                    "population_millions": population,
                    "forecast_year": forecast_year,
                }
            ]
        )
        row = clean_data(row)
        X = row[feature_columns]
        pred = float(max(model_tabular.predict(X)[0], 0))
        tab_metrics = (metrics_cached or {}).get("tabular", {})
        return jsonify(
            {
                "predictions": [pred],
                "model_version": tab_metrics.get("model", "random_forest"),
                "metrics": {"mae": tab_metrics.get("mae"), "rmse": tab_metrics.get("rmse")},
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 400


def _timeseries_forecast(history: list[dict], horizon: int):
    history = sorted(history, key=lambda h: h["year"])
    years = np.array([int(h["year"]) for h in history], dtype=float)
    ys = np.array([float(h["disposal_amount_tonnes"]) for h in history], dtype=float)

    if len(ys) < 2:
        raise ValueError("Need at least 2 historical points")

    last_year = int(years[-1])
    series = []

    if len(ys) >= 8:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                model = ARIMA(ys, order=(1, 1, 1))
                fit = model.fit()
                fc = fit.get_forecast(steps=horizon)
                pred_mean = fc.predicted_mean
                fv = np.asarray(fit.fittedvalues, dtype=float)
                mae = float(mean_absolute_error(ys, fv))
                rmse = float(np.sqrt(mean_squared_error(ys, fv)))
            for i in range(horizon):
                val = float(pred_mean.iloc[i]) if hasattr(pred_mean, "iloc") else float(pred_mean[i])
                series.append({"year": last_year + i + 1, "predicted_tonnes": float(max(val, 0))})
            return series, {"mae": mae, "rmse": rmse}, "arima"
        except Exception:
            pass

    coef = np.polyfit(years, ys, 1)
    fitted = np.polyval(coef, years)
    mae = float(mean_absolute_error(ys, fitted))
    rmse = float(np.sqrt(mean_squared_error(ys, fitted)))
    for h in range(1, horizon + 1):
        y = last_year + h
        p = max(float(np.polyval(coef, y)), 0)
        series.append({"year": y, "predicted_tonnes": p})
    return series, {"mae": mae, "rmse": rmse}, "linear_trend"


@app.route("/forecast/timeseries", methods=["POST"])
def forecast_timeseries():
    try:
        body = request.json or {}
        history = body.get("history") or []
        horizon = int(body.get("horizon_years", 5))
        if horizon < 1 or horizon > 30:
            return jsonify({"error": "horizon_years must be 1-30"}), 400
        series, metrics, name = _timeseries_forecast(history, horizon)
        return jsonify(
            {
                "series": series,
                "metrics": metrics,
                "model_version": name,
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/spatial/clusters", methods=["POST"])
def spatial_clusters():
    try:
        body = request.json or {}
        points = body.get("points") or []
        method = body.get("method", "dbscan")
        if len(points) == 0:
            return jsonify({"geojson": {"type": "FeatureCollection", "features": []}})

        coords = np.array([[float(p["lat"]), float(p["lng"])] for p in points])
        scaler = StandardScaler()
        X = scaler.fit_transform(coords)
        weights = np.array([float(p.get("weight", 1) or 1) for p in points])

        if method == "kmeans":
            k = min(4, max(1, len(points)))
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            try:
                km.fit(X, sample_weight=weights)
            except TypeError:
                km.fit(X)
            labels = km.predict(X)
        else:
            try:
                d = DBSCAN(eps=0.55, min_samples=1)
                d.fit(X, sample_weight=weights)
                labels = d.labels_
            except TypeError:
                d = DBSCAN(eps=0.55, min_samples=1)
                d.fit(X)
                labels = d.labels_

        features = []
        for i, p in enumerate(points):
            features.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(p["lng"]), float(p["lat"])],
                    },
                    "properties": {
                        "cluster": int(labels[i]),
                        "region": p.get("region", ""),
                        "weight": float(p.get("weight", 1) or 1),
                    },
                }
            )
        return jsonify({"geojson": {"type": "FeatureCollection", "features": features}})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
