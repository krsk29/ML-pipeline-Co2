# ML-Pipeline-Co2
## End-to-End Machine Learning Pipeline for Predicting CO2 Emissions in Construction Logistics

This project develops an end-to-end machine learning pipeline to predict CO2 emissions in construction logistics. It leverages PySpark, Apache Airflow, and Apache Beam to simulate a big data environment and production at scale.

### Objective
The aim is to create a robust pipeline capable of handling large-scale data, simulating real-world scenarios in construction logistics, and providing accurate predictions of CO2 emissions.

---

## Data Ingestion and Storage

### Data Source Simulation
- **Script-Generated Data**: Simulate real-time data streams.
- **Periodic Data Generation**: Push data into the pipeline at regular intervals to mimic real-world data flow.
- **Local PostgreSQL Database**: Manually populated with initial data sets. Future updates will introduce more data to simulate evolving datasets.

### Local Storage Solution
- **Filesystem-Based Approach**: Emulate a data lake structure for scalability.
- **Local PostgreSQL Database**: Implemented for structured data management.

### Scripts to Process Data
- **Data Ingestion Script**: `data_ingestion.py` for efficient data extraction and preparation.
- **Data Merging Script**: `data_merging.py` to merge the 4 datasets ingested from data_ingestion.py - this produces a unique joined dataset for preprocessing and modelling.
---

## PostgreSQL Database Configuration and Data Import

### Setting up PostgreSQL
- **Installation**: Install PostgreSQL in the environment.
- **Database and User Creation**: Specific to the project.

### Configuring PostgreSQL Authentication
- **Authentication Method**: Utilize `scram-sha-256` for security.
- **Configuration File Location**: `/etc/postgresql/<version>/main/pg_hba.conf`.
- **Service Restart**: Execute `sudo service postgresql restart` after modifications.

### Configure Postgres Drivers
- **Create a drivers folder in the root directory**: place the postgresql driver pgJDBC in the root dir. Download it from the website (https://jdbc.postgresql.org/download/). You will need this driver to post data to your local db. It is used in `data_merging.py`

### Data Import Script
- **Shell Script**: `run_sql.sh` for importing CSV data.
- **Usage**: Replace paths in the script with actual data and SQL file locations.
- **Execution**:
   ```bash
   #!/bin/bash
   CSV_PATH="/your/path/to/logistics_data.csv" # Replace with your path
   SQL_FILE="/your/path/to/sql/import_script.sql" # Replace with your path

   # Replace placeholder in SQL file and execute
   sed "s|<PATH_TO_CSV>|${CSV_PATH}|g" $SQL_FILE | psql -U your_username -d your_database
- **Script Permission**: Make executable with chmod +x run_sql.sh.
- **Run**: Execute with ./run_sql.sh.

### Environment and Security
- **.env File**: For secure database credential managemen and for variables used in the project
- **Security Note**: Exclude .env from version control for privacy.## Apache Airflow Integration


## Airflow Set-up
Integration of Apache Airflow to enhance the automation, scheduling, and monitoring of pipeline workflows.

### 1. Installation
- Installed Apache Airflow within the project's virtual environment to maintain an isolated setup.
- Used the command `pip install apache-airflow` for installation.

### 2. Initializing Airflow
- Ran `airflow db init` to initialize Airflow's metadata database.
- This command created the default `~/airflow` directory, housing the configuration and DAG files.

### 3. PostgreSQL Configuration (In Progress)
- Plan to configure Airflow to use PostgreSQL instead of the default SQLite.
- Will modify the `sql_alchemy_conn` line in `airflow.cfg` to point to our PostgreSQL database.

### Next Steps

- Complete the PostgreSQL database configuration for Airflow.
- Create DAGs to manage and automate tasks in our data pipeline, including data ingestion and merging.
- Test and validate the setup to ensure efficient workflow management.

### Note
The integration process is currently in progress and will continue with further configurations and the implementation of DAGs for task automation.
