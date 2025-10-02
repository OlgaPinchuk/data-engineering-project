import joblib
import gcsfs
import pandas as pd
from google.cloud import bigquery

def model_load():
    fs = gcsfs.GCSFileSystem()
    with fs.open("team-weather-ml-model/model.pkl", "rb") as f:
        model = joblib.load(f)
    print("Model loaded successfully!")
    return model

def load_data(project_id: str, table_id: str, columns: list):
    client = bigquery.Client(project=project_id, location="EU")
    cols = ", ".join(columns)
    query = f"""
        SELECT {cols}
        FROM `{table_id}`
    """
    df = client.query(query).to_dataframe()
    print(f"Loaded {len(df)} rows with columns: {df.columns.tolist()}")
    return df

def model_predict(model, data: pd.DataFrame):
    feature_cols = ['hour', 'month', 'temp', 'humidity', 'pressure', 'temp_lag_1', 'temp_lag_3']
    X = data[feature_cols]
    preds = model.predict(X)
    return pd.Series(preds, index=data.index, name='predicted_temp')

def save_predictions(df: pd.DataFrame, project_id: str, dataset_id: str, table_name: str):
    """
    Saves predictions DataFrame to BigQuery
    """
    client = bigquery.Client(project=project_id, location="EU")
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for the load to complete
    print(f"Predictions saved to {table_id}")

if __name__ == "__main__":
    # Load model
    model = model_load()

    # Load data
    project_id = "team-weather-ml-project"
    table_id = "team-weather-ml-project.dbt_dev_version_2_dbt_data.processed_weather_data"
    columns = ["dt_iso", "hour", "month", "temp", "humidity", "pressure", "temp_lag_1", "temp_lag_3"]

    df = load_data(project_id, table_id, columns)

    # Generate predictions
    df['predicted_temp'] = model_predict(model, df)

    df = df.sort_values(by=['dt_iso', 'hour']).reset_index(drop=True)

    # Save predictions back to BigQuery
    save_predictions(df, project_id, "dbt_dev_version_2_dbt_data", "predicted_weather_data")