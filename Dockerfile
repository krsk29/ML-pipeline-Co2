# Use Python Alpine image for the builder stage
FROM python:3.10.13-alpine3.19 as builder

# Install build dependencies
RUN apk add --no-cache build-base libffi-dev

# Install packages and dependencies
RUN pip install --no-cache-dir mlflow==2.10.2 pyspark==3.5.0

# Multi-stage build: Start with a fresh Alpine image for the final build
FROM python:3.10.13-alpine3.19

# Copy installed Python packages from the builder stage
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages

# Create directory inside container for the app
WORKDIR /app

# Copy necessary directories from the project dir
COPY src ./src
COPY mlartifacts ./mlartifacts
COPY mlruns ./mlruns

# Expose container port that MLflow will use
EXPOSE 5000

# Set entrypoint to serve the model
ENTRYPOINT ["mlflow", "models", "serve", "--model-uri", "/app/mlartifacts/436141932004513545/f5310927afe44cc09339b51236000eb2", "-h", "0.0.0.0", "-p", "5000", "--no-conda"]
