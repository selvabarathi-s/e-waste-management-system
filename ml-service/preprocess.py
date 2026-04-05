import pandas as pd


def clean_data(df):
    """Coerce numeric columns and fill NaN (safe for single-row inference)."""
    df = df.copy()
    for col in ("sales_import_tonnes", "population_millions", "forecast_year"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.fillna(0)
