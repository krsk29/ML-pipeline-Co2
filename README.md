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

### Utility Script
`script_utils.py` to manage environamental variables and loggin setup
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

### 3. PostgreSQL Configuration
- Configured Airflow to use PostgreSQL instead of the default SQLite for improved performance, concurrency handling, and scalability in production environments.
- Created a dedicated PostgreSQL database and user (airflow_db and airflow_user) for Airflow.
- Modified the `sql_alchemy_conn` line in `airflow.cfg` within the project's `airflow` directory to connect to the PostgreSQL database.
- Set `AIRFLOW_HOME` to the project's `airflow` directory and re-initialized Airflow with `airflow db init` for a project-centric approach, enhancing manageability and collaboration.
  
### Set-up contd...  
- Set-up Airflow authentication by running this code in the environemnt where Airflow is installed:
    ```bash
    airflow users create \
    --username [your_username] \
    --firstname [Your First Name] \
    --lastname [Your Last Name] \
    --role Admin \
    --email [your_email@example.com] \
    --password [your_password]
- Start Airflow webserver with `airflow webserver -p 8080` and access airflow UI from browser on `http://localhost:8080`

### Next Steps

- Develop DAGs to manage and automate tasks in our data pipeline, including data ingestion and merging.
- Test and validate the setup to ensure efficient workflow management and adapt to production needs.

### Note
The integration process is ongoing and will include further configurations and the implementation of DAGs for enhanced task automation and pipeline management.