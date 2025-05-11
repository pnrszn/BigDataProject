FROM python:3.10-slim
WORKDIR /app
COPY . /app
COPY certs/root.crt /root/.postgresql/root.crt
COPY wait-for-kafka.sh /app/wait-for-kafka.sh
# Install netcat-openbsd and other dependencies
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*
RUN chmod +x /app/wait-for-kafka.sh
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "kafka_consumer.py"]