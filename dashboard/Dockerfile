FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir flask==3.1.3 kafka-python==2.3.2

# Copy dashboard files
COPY index.html pipeline.html server.py ./

# Mount point for project data (historical CSV, reports)
# Actual data is bind-mounted by docker-compose
VOLUME ["/app/data"]

ENV DASHBOARD_PORT=8766
ENV KAFKA_BOOTSTRAP=kafka:29092
ENV KAFKA_TOPIC=air_quality_normalized

EXPOSE 8766

CMD ["python", "server.py"]
