FROM python:3.10-slim as builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential libffi-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir mlflow==2.10.2 pyspark==3.5.0

FROM python:3.10-slim

COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages

# Create directory inside container for the app
WORKDIR /app

COPY src ./src
COPY mlartifacts ./mlartifacts
COPY mlruns ./mlruns

EXPOSE 5000

# Set entrypoint to serve the model
ENTRYPOINT ["mlflow", "models", "serve", "--model-uri", "/app/mlartifacts/436141932004513545/f5310927afe44cc09339b51236000eb2", "-h", "0.0.0.0", "-p", "5000", "--no-conda"]
