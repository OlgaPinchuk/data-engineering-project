import os
import json
from datetime import datetime, date, timezone, timedelta

import logging
from typing import Any, Dict
import requests

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from dotenv import load_dotenv


load_dotenv()

API_KEY = os.environ["API_KEY"]
LOCATION = os.environ.get("LOCATION", "Stockholm")
DATE = os.environ.get("DATE", (date.today() - timedelta(days=1)).isoformat())

URL = "https://api.weatherapi.com/v1/history.json"


# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataPipeline:
    def __init__(self):
        """Initialize the data pipeline with BigQuery client and configuration"""
        self.client = bigquery.Client()
        self.project_id = os.environ.get("GCP_PROJECT_ID")
        self.dataset_id = os.environ.get("BQ_DATASET_ID")
        self.table_id = os.environ.get("BQ_TABLE_ID", "weather_data")

        logger.info(f"Pipeline initialized for project: {self.project_id}")

    def build_source_url(self, location: str, date: str) -> str:
        return f"{URL}?q={location}&dt={date}"

    def fetch_data(self) -> Dict[str, Any]:
        """Fetch data from external API"""
        params = {"key": API_KEY, "q": LOCATION, "dt": DATE}

        try:
            logger.info(f"Fetching data for {LOCATION} on {DATE}...")
            response = requests.get(URL, params=params, timeout=30)
            response.raise_for_status()

            try:
                data = response.json()
            except ValueError as json_err:
                logger.error("Failed to parse JSON response.")
                raise ValueError("Invalid JSON response") from json_err

            if not data:
                raise ValueError("Empty response from API")

            logger.info(f"Successfully fetched {len(data)} records")
            return data

        except requests.Timeout:
            logger.error("Request to API timed out.")
            raise
        except requests.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err} - Response: {response.text}")
            raise
        except requests.RequestException as req_err:
            logger.error(f"Error fetching data from API: {req_err}")
            raise

    def record_exists(self, source_url: str) -> bool:
        """Checks if a record for the given source_url already exists in BigQuery"""
        query = f"""
            SELECT COUNT(*) as count
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE source_url = @source_url
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source_url", "STRING", source_url)
            ]
        )

        logger.info("Checking if record already exists in BigQuery...")

        query_job = self.client.query(query, job_config=job_config)
        result = query_job.result()

        count = next(result).count
        exists = count > 0

        logger.info(f"Record exists: {exists}")
        return exists

    def write_raw(
        self, payload: Dict[str, Any], source_url: str, fetched_at: str
    ) -> Dict[str, Any]:
        """Writes raw payload into BigQuery with metadata."""
        table_id = self.client.dataset(self.dataset_id).table(self.table_id)

        row = {
            "raw_json": json.dumps(payload, ensure_ascii=False),
            "source_url": source_url,
            "fetched_at": fetched_at,
        }
        row_id = f"{source_url}:{fetched_at}"
        logger.info(f"Writing row to {table_id}")

        try:
            errors = self.client.insert_rows_json(table_id, [row], row_ids=[row_id])
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                raise GoogleAPIError(f"BigQuery insert failed: {errors}")

            logger.info("Row successfully inserted into BigQuery.")
            return {"table": table_id, "inserted": 1}

        except GoogleAPIError as e:
            logger.exception("BigQuery insert failed.")
            raise

    def run_pipeline(self):
        """Execute the complete data pipeline"""
        try:
            logger.info("=" * 50)
            logger.info("Starting Data Pipeline")
            logger.info("=" * 50)

            # Fetch data
            payload = self.fetch_data()
            fetched_at = datetime.now(timezone.utc).isoformat()
            source_url = self.build_source_url(LOCATION, DATE)

            # Check if data already exists
            if self.record_exists(source_url):
                logger.info("Data already exists in BigQuery. Skipping insert.")
                return {
                    "status": "skipped",
                    "reason": "Record already exists",
                    "location": LOCATION,
                    "date": DATE,
                }

            # Upload to BigQuery
            logger.info("Writing to BigQuery")
            result = self.write_raw(payload, source_url, fetched_at)

            return {"status": "ok", "location": LOCATION, "date": DATE, **result}

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_pipeline()
