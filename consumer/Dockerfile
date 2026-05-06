FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python==2.0.2 psycopg2-binary==2.9.9

COPY crypto_consumer.py .

# Overridden by docker-compose environment block
ENV KAFKA_BROKER=kafka:29092
ENV DB_HOST=postgres
ENV DB_PORT=5432
ENV DB_NAME=streamdb
ENV DB_USER=streamuser
ENV DB_PASSWORD=streampass

CMD ["python", "crypto_consumer.py"]
