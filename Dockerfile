FROM mcr.microsoft.com/playwright/python:v1.49.0-noble

WORKDIR /app

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Expose Render standard port
EXPOSE 10000

# Start Uvicorn bound to 0.0.0.0
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
