FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY consumption_pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY consumption_pipeline/app_final.py consumption_pipeline/app_final.py

# Run the application
CMD ["python", "consumption_pipeline/app_final.py"]