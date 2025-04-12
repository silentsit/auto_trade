FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
# Use PORT environment variable from Render or default to 8000
ENV PORT=${PORT:-8000}

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY *.py /app/
COPY fx/ /app/fx/
# Don't copy .env in the Dockerfile to ensure environment variables
# are set through Render's environment variable system

# Make sure log and data directories exist
RUN mkdir -p /app/logs /app/data

# Create a non-root user for better security
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Expose the port that will be used by the application
# (Render will automatically set the PORT environment variable)
EXPOSE $PORT

# Use CMD format that properly handles environment variables at runtime
# This allows Render to set PORT dynamically
CMD uvicorn enhanced_trading:app --host 0.0.0.0 --port $PORT 