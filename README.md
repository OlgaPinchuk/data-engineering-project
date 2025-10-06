# Data Engineering Project: Weather AB

## Table of Contents

1. [Project Overview](#project-overview)

2. [Architecture / Components](#architecture--components)

3. [Getting Started](#getting-started)

4. [License](#license)

## Project Overview

This project aims to build a complete data engineering stack including ingestion, transformation and consumption layers using tools such as GCP, dbt etc.

The goal of this project was to create a data platform capable of automatically ingesting and processing live weather data from a public Weather API, preparing it for future retraining and analytics of exsisting ML model. The model should be deployed to the cloud for end-user access.

Some key goals:

- Create a robust ingestion pipeline

- Define transformations in a structured, testable way (via dbt)

- Provide downstream data consumption points (ML-ready tables and dashboards)

- Support development best practices (testing, modularization, version control)

- Use Agile approach

## Architecture / Components

The repository is partitioned into these major parts:

- `ingestion_pipeline/` — handles extracting or collecting raw data

- `transformation_pipeline/` (a dbt project) — data modeling and cleaning

- `consumption_pipeline/` — logic for making transformed data available for further analysing (syncing to BI tools)

## Getting Started

### Prerequisites
You’ll need the following installed:

- Python (version 3.11 or later)

- pip

- dbt 

- GCP Account

- Access / credentials to data sources and targets

### Installation

1. Clone the repository:

    ```
    git clone https://github.com/OlgaPinchuk/data-engineering-project.git

    cd data-engineering-project
    ```

2. Create & activate a Python virtual environment:

    ```
    python3 -m venv venv

    source venv/bin/activate
    ```

3. Install dependencies:
    `pip install -r requirements.txt`

4. Install dbt packages:

    ```
    cd transformation_pipeline/dbt_group_project

    dbt deps
    ```

### Running the Pipelines

Each pipeline in this project is containerized and can be built and deployed independently.

Every pipeline directory (`ingestion_pipeline`, `transformation_pipeline`, `consumption_pipeline`) includes its own Dockerfile and cloudbuild.yaml configuration for Google Cloud Build.

## License

This project is licensed under the MIT License.