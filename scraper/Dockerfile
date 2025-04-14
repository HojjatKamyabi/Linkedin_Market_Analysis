FROM python:3.10-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    wget unzip curl gnupg2 \
    chromium-driver chromium \
    && apt-get clean

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /app

COPY . .

CMD ["python", "main.py"]
