import os
import datetime as dt
import logging
import requests

from google.cloud import bigquery

import pandas as pd


from dotenv import load_dotenv


load_dotenv()

API_KEY = os.environ["API_KEY"]
LOCATION = os.environ.get("LOCATION", "Stockholm")
DATE = os.environ.get("DATE", dt.date.today().isoformat())


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

    def fetch_data(self):
        """Fetch data from external API"""
        try:
            logger.info("Fetching data from Weather API...")
            url = "https://api.weatherapi.com/v1/history.json"
            params = {"key": API_KEY, "q": LOCATION, "dt": DATE}
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            logger.info(f"Successfully fetched {len(data)} records")
            return data

        except requests.RequestException as e:
            logger.error(f"Error fetching data: {e}")
            raise

    def upload_to_bigquery(self, data):
        """Upload data to BigQuery table"""
        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)

        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            logger.info(f"Prepared {len(df)} records for upload")

            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND", autodetect=True
            )

            # Upload data
            logger.info("Starting BigQuery upload...")
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )

            # Wait for completion
            job.result()

            # Get updated table info
            table = self.client.get_table(table_ref)
            logger.info(f"Upload complete! Table now has {table.num_rows} total rows")

        except Exception as e:
            logger.error(f"Error uploading to BigQuery: {e}")
            raise

    def run_pipeline(self):
        """Execute the complete data pipeline"""
        try:
            logger.info("=" * 50)
            logger.info("Starting Data Pipeline")
            logger.info("=" * 50)

            # Fetch data
            data = self.fetch_data()

            # Upload to BigQuery
            self.upload_to_bigquery(data)

            logger.info("=" * 50)
            logger.info("Pipeline completed successfully!")
            logger.info("=" * 50)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_pipeline()
