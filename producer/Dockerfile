FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python==2.0.2 requests==2.31.0

COPY crypto_producer.py .

# Overridden by docker-compose environment block
ENV KAFKA_BROKER=kafka:29092
ENV POLL_INTERVAL_SECONDS=10

CMD ["python", "crypto_producer.py"]
