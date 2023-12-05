# ML-pipeline-Co2
End-to-end ML pipeline predicting Co2 emissions in construction logistics. Uses PySpark, Apache Airflow, Apache Beam.

## The ambition here is to simulate a big data environment and production at scale

# Data Ingestion and Storage
Data Source Simulation:
- Use script-generated data
- Simulate real-time data streams by periodically generating data and pushing it into the ingestion pipeline.

Local Storage Solution:
- Use a filesystem-based approach for simplicity and local constraints.
- Structure the directory in a format that mimics a data lake structure for scalability simulation

Database for Processed Data:
- Use a local SQL database like SQLite or PostgreSQL for structured data.
- Consider incorporating a local NoSQL database like MongoDB for complex or semi-structured data.