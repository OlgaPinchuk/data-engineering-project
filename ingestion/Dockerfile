FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY ingestion/api.py ingestion/api.py
COPY service-account-key.json .

# Run the application
CMD ["python", "ingestion/api.py"]