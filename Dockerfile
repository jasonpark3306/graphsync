FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including tk
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    python3-tk \
    tk-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]