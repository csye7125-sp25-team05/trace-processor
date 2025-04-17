FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        build-essential \
        libffi-dev \
        poppler-utils \
        # Dependencies for PDF processing
        python3-dev \
        python3-pip \
        python3-setuptools \
        python3-wheel \
        # Clean up to reduce image size
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY pdf-processor.py .



# Run the processor
CMD ["python", "pdf_processor.py"]
