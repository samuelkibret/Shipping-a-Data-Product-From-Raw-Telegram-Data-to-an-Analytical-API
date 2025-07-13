FROM python:3.10-slim-buster

# Working directory in the container
WORKDIR /app

# Install system dependencies required by psycopg2-binary (PostgreSQL client libraries)
# and other potential build tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of application code into the container
# This assumes Python scripts will be in the /app directory
COPY . .

# Expose the port FastAPI will run on (default for Uvicorn)
EXPOSE 8000

# Command to run FastAPI application (this will be overridden by docker-compose usually)
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]