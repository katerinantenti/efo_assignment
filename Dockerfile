# ============================================================================
# EFO Data Pipeline - Docker Image
# ============================================================================
# Python 3.11 slim base image for minimal size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies first (leverages Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY src/ ./src/

# Set Python to run in unbuffered mode (better for logging in containers)
ENV PYTHONUNBUFFERED=1

# Default command - run the pipeline
CMD ["python", "-m", "src.pipeline"]

