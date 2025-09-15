# Access Control

## Project: DataEngineering Team Project

| Name             | Identity                                           | IAM Role                          | Level   | Description / Notes                     |
|------------------|----------------------------------------------------|-----------------------------------|---------|-----------------------------------------|
| Olha Pinchuk     | pinchukolya@gmail.com                              | roles/owner                       | Project | Full access to all resources            |
| Mariia Sauta     | ugabitan@gmail.com                                 | roles/editor                      | Project | Can create/edit resources across GCP    |
|                  |                                                    | roles/storage.objectAdmin         | Project | Full control over Cloud Storage objects |

--

## 📊 BigQuery Dataset: `team_weather_ml_raw`

| Name                  =| Identity                                                          | IAM Role                      | Level   | Description                          |
|----------------------|---------------------------------------------------------------------|-------------------------------|---------|--------------------------------------|
| Mariia Sauta         | ugabitan@gmail.com                                                  | roles/bigquery.dataEditor     | Dataset | Can insert, update, and delete data  |
|                      |                                                                     | roles/bigquery.jobUser        | Dataset | Can run queries and jobs in BigQuery |
| Weather ML Service   | team-weather-ml-sa@team-weather-ml-project.iam.gserviceaccount.com  | roles/bigquery.dataEditor     | Dataset | Can insert, update, and delete data  |
|                      |                                                                     | roles/bigquery.jobUser        | Dataset | Can run queries and jobs in BigQuery |
