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


#### PLANNING DESIGN FOR PRODUCTION DEPLOYMENT ####

### PostgreSQL Database Configuration and Data Import

#### Setting up PostgreSQL
- Install PostgreSQL within the environment.
- Create a PostgreSQL database and user for the project.

#### Configuring PostgreSQL Authentication
- Modify the PostgreSQL `pg_hba.conf` file to use `scram-sha-256` authentication for increased security.
- Location of `pg_hba.conf`: `/etc/postgresql/<version>/main/pg_hba.conf` (replace `<version>` with your PostgreSQL version).
- After modifying `pg_hba.conf`, restart the PostgreSQL service using `sudo service postgresql restart`.

#### Data Import Script
- Use `run_sql.sh` shell script to import CSV data into the PostgreSQL database.
- The script replaces placeholders in the SQL import script with actual file paths and executes the SQL command.
1. **Create a Shell Script** (e.g., `run_sql.sh`) in your project root.
2. **Script Content**:
   ```bash
   #!/bin/bash
   CSV_PATH="/your/path/to/logistics_data.csv"  # Replace with your path
   SQL_FILE="/your/path/to/sql/import_script.sql"  # Replace with your path

   # Replace placeholder in SQL file and execute
   sed "s|<PATH_TO_CSV>|${CSV_PATH}|g" $SQL_FILE | psql -U your_username -d your_database

- Make the script executable with `chmod +x run_sql.sh` and run it with `./run_sql.sh`.