FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY ingestion_pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY ingestion_pipeline/api.py ingestion_pipeline/api.py

# Run the application
CMD ["python", "ingestion_pipeline/api.py"]