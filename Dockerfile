# Use an official Python runtime based on a recent stable Debian release (Bookworm)
FROM python:3.12-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Prevent Python from writing .pyc files to disc
ENV PYTHONDONTWRITEBYTECODE 1
# Ensure stdout/stderr are not buffered
ENV PYTHONUNBUFFERED 1

# Install system dependencies for Python packages, including a comprehensive set for OpenCV (cv2)
# These are common dependencies for headless OpenCV operation on Debian-based systems.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libfontconfig1 \
    libxext6 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Command to keep the container running (will be overridden by docker-compose)
CMD ["tail", "-f", "/dev/null"]