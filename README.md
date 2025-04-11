# Traffic Accidents ETL

## General Description

The **Traffic Accidents ETL** project is an advanced data pipeline solution designed to process, analyze, and visualize traffic accident data. Initially developed as a Python-based ETL pipeline, this second iteration enhances the project by integrating **Apache Airflow** for workflow orchestration, improving scalability, scheduling, and monitoring capabilities. The pipeline extracts data from a Kaggle CSV dataset (migrated to a PostgreSQL database) and enriches it with geospatial data from OpenStreetMap, enabling detailed analysis of accident patterns, infrastructure correlations, and risk zones. The transformed data is stored in a dimensional model and visualized through an interactive Power BI dashboard, providing actionable insights for urban planning and road safety.

## Objectives

- **Extract**: Collect traffic accident data from a CSV file sourced from Kaggle, migrated to a PostgreSQL database (`CrashTraffic` table), and retrieve complementary geospatial data from the OpenStreetMap (OSM) API.
- **Transform**: Clean, enrich, and structure the raw data into a dimensional model optimized for analytical queries.
- **Load**: Store the transformed data into a new PostgreSQL database (`CrashTraffic_Dimensional`) for downstream analytics.
- **Orchestrate**: Automate and manage the ETL pipeline using Apache Airflow, ensuring reliability and scalability.
- **Analyze**: Perform exploratory data analysis (EDA) on both the traffic accident dataset and OpenStreetMap data to identify patterns, trends, and integration opportunities.
- **Visualize**: Create an interactive Power BI dashboard to display key insights, such as accident severity, infrastructure correlations, and temporal trends.

## Data Source

The primary dataset is sourced from Kaggle: *Traffic Accidents*. It contains over 10,000 records with features like accident severity, weather conditions, lighting conditions, and temporal data. The raw CSV data (`CrashTraffic.csv`) is migrated to a PostgreSQL database (`CrashTraffic` table) for processing. Additionally, geospatial data from OpenStreetMap is used to enrich the dataset with location-based features, such as proximity to hospitals, schools, or intersections.

## ETL Pipeline

### Extraction
- **Source**: Traffic accident data is extracted from the `CrashTraffic` table in a PostgreSQL database, which was populated from the Kaggle CSV file (`CrashTraffic.csv`). Geospatial data is retrieved from the OpenStreetMap API.
- **Method**: Data is extracted using Python scripts and SQL queries, with OpenStreetMap data processed in the `API_EDA.ipynb` notebook.

### Transformation
The transformation step involves cleaning, enriching, and structuring the data:
- **Data Cleaning**:
  - Removal of null values and duplicates.
  - Standardization of categorical columns (e.g., replacing "UNKNOWN" with "OTHER" in `weather_condition` and `road_defect`).
- **Feature Engineering**:
  - Conversion of `crash_date` to datetime format for temporal analysis.
  - Transformation of `most_severe_injury` into an ordered categorical variable with levels: `NO INDICATION OF INJURY`, `REPORTED, NOT EVIDENT`, `NON-INCAPACITATING INJURY`, `INCAPACITATING INJURY`, `FATAL`.
  - Conversion of `intersection_related` to a binary indicator (1 for 'Y', 0 otherwise).
- **Enrichment**: Integration of OpenStreetMap data to add geospatial features.
- **Dimensional Model**: The data is structured into a star schema for efficient querying, stored in the `CrashTraffic_Dimensional` database.
- **Tools**: Transformations are performed in Jupyter Notebooks (`001_extract.ipynb`, `002_EDA_csv.ipynb`, `API_EDA.ipynb`) and orchestrated via Apache Airflow.

### Loading
- **Destination**: The transformed data is loaded into the `CrashTraffic_Dimensional` database in PostgreSQL.
- **Method**: The cleaned and structured data is written to the database using SQLAlchemy, with the process managed by an Airflow DAG (`etl_crash_traffic.py`).

### Orchestration
- **Apache Airflow**: The ETL pipeline is automated using Airflow, with the `etl_crash_traffic.py` DAG defining tasks:
  - `setup_tables`: Creates tables in the `CrashTraffic_Dimensional` database.
  - `Extract`: Retrieves data from the `CrashTraffic` table.
  - `Transform`: Cleans and enriches the data.
  - `Load`: Stores the transformed data into `CrashTraffic_Dimensional`.

## Exploratory Data Analysis (EDA)
EDA is conducted in two stages:
- **API EDA** (`API_EDA.ipynb`): Analyzes raw geospatial data from OpenStreetMap to assess quality, consistency, and relevance.
- **Dataset EDA** (`002_EDA_csv.ipynb`): Validates the cleaned traffic accident dataset, focusing on:
  - Data summary (column types, non-null counts, descriptive statistics).
  - Univariate analysis (e.g., distribution of `weather_condition`, `most_severe_injury`).
  - Bivariate analysis (e.g., `weather_condition` vs. `injuries_total`).
  - Temporal analysis (trends by `crash_hour`, `crash_day_of_week`, `crash_month`).

## Visualizations
An interactive Power BI dashboard visualizes key insights, including:
- **Accident Severity by Type of Road**: Distribution of accidents by road type and severity.
- **Weather and Lighting Conditions**: Impact of weather and lighting on injuries.
- **Evolution of Injuries by Year**: Temporal trends in injury counts.
- **Fatality by Location**: Geographic distribution of fatal accidents.
- **Infrastructure Correlations**: Relationships between accidents and infrastructure (e.g., lack of traffic signals).
- **Trends in Injuries Over Time**: Longitudinal analysis of injury patterns.

The dashboard offers filters for interactive exploration and is accessible after the pipeline updates the `CrashTraffic_Dimensional` database.

## Project Structure

```
📂 Traffic-Accidents-ETL
TRAFFICACCIDENTS/
├── API/
│   ├── data/
│   ├── notebooks/
│   │   ├── API_EDA.ipynb
│   │   └── git_attributes
├── config/
│   └── conexion_db.py
├── dags/
│   └── etl_crash_traffic.py
├── Dashboard/
├── data/
│   ├── CrashTraffic_clean.csv
│   └── CrashTraffic.csv
├─  logs/
├── notebooks/
│   ├── 001_extract.ipynb
│   ├── 002_EDA_csv.ipynb
├── pdf/
├── venv/
├── .env
├── .gitignore
├── docker-compose.yaml
├── README.md
└── requirements.txt
```

## Technologies Used
- **Programming**: Python
- **Data Manipulation**: pandas, numpy
- **Visualization**: matplotlib, seaborn, Power BI Desktop
- **Database**: PostgreSQL, psycopg2, SQLAlchemy
- **Workflow Orchestration**: Apache Airflow
- **Containerization**: Docker, Docker Compose
- **Environment**: Jupyter Notebook
- **Version Control**: Git, GitHub

## Final Data Destination
- **Database**: The transformed data is stored in the `CrashTraffic_Dimensional` database in PostgreSQL, structured as a dimensional model.
- **Dashboard**: Visualized in Power BI for interactive insights, accessible after pipeline execution.

## Performance and Findings
- **Efficiency**: The pipeline handles large datasets (>10k rows) with optimized operations in Python, SQL, and Airflow.
- **Key Findings**:
  - Adverse weather and lighting conditions correlate with higher injury rates.
  - Peak accident times (e.g., rush hours) and days (e.g., weekends) were identified.
  - Infrastructure issues, such as lack of traffic signals, are linked to higher accident rates.
  - Geospatial analysis highlights risk zones near schools and hospitals, supporting targeted interventions.

## Project Requirement Alignment
This project meets the requirements of the "ETL Project - Second Delivery":
- **Dataset**: Over 10,000 rows from Kaggle, enriched with OpenStreetMap data.
- **ETL**: Migration from CSV to PostgreSQL, transformation into a dimensional model, and loading into a new database.
- **Orchestration**: Automated using Apache Airflow with a defined DAG.
- **EDA**: Conducted on both traffic accident and OpenStreetMap data in Jupyter Notebooks.
- **Visualizations**: Generated from the database in Power BI, with advanced geospatial insights.
- **Technologies**: Use of Python, Jupyter, PostgreSQL, Airflow, Docker, Power BI, and GitHub.

## Installation

To set up this project on your local machine, follow these steps:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/isabellaperezcav/Traffic-Accidents-ETL.git
   cd Traffic-Accidents-ETL
   ```

2. **Create and Activate a Virtual Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set Up the PostgreSQL Database**:
   - Ensure PostgreSQL is installed and running.
   - Create databases for the project:
     ```sql
     CREATE DATABASE crash_traffic;
     CREATE DATABASE crash_traffic_dimensional;
     ```
   - Update the connection credentials in `API/config/conexion_db.py`.

5. **Configure Environment Variables**:
   - Create a `.env` file in the `venv/` directory with the following:
     ```
     AIRFLOW__CORE__EXECUTOR=SequentialExecutor
     AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:password@localhost:5432/airflow
     DB_HOST=localhost
     DB_PORT=5432
     DB_USER=your_user
     DB_NAME=your_db
     DB_PASSWORD=your_password
     ```

6. **Set Up Airflow with Docker**:
   - Start Airflow services using Docker Compose:
     ```bash
     docker-compose up -d
     ```
   - Access the Airflow web interface at `http://localhost:8080`.

7. **Run the Notebooks**:
   - Open and run `notebooks/001_extract.ipynb` to migrate the CSV to the `CrashTraffic` database.
   - Open and run `notebooks/002_EDA_csv.ipynb` for EDA on the traffic accident data.
   - Open and run `API/notebooks/API_EDA.ipynb` for EDA on OpenStreetMap data.

8. **Run the ETL Pipeline**:
   - Enable the `etl_crash_traffic` DAG in the Airflow UI and trigger it manually or let it run on schedule.

9. **View the Dashboard**:
   - After the pipeline completes, access the Power BI dashboard to explore visualizations, ensuring the database connection is configured.

## Usage

- **Running the ETL Pipeline**:
  - In the Airflow UI, navigate to the DAGs section, toggle the `etl_crash_traffic` DAG to "On," and click "Trigger DAG" to run it manually.
- **Viewing the Dashboard**:
  - Access the Power BI dashboard to explore visualizations after the pipeline updates the `CrashTraffic_Dimensional` database.


## Maintenance and Troubleshooting

- **Monitoring**: Use the Airflow web interface to monitor DAG runs, task statuses, and logs (`API/logs/` directory).
- **Common Issues**:
  - **Database Connection Errors**: Verify credentials in `.env` and `conexion_db.py`.
  - **Airflow Task Failures**: Ensure dependencies are installed (`requirements.txt`) and the DAG file is syntactically correct.
  - **API Rate Limits**: Implement retry logic in the extraction step for OpenStreetMap API calls.
- **Updates**: Modify the `etl_crash_traffic.py` DAG and redeploy via Airflow to update the pipeline.

## Conclusion

The "Traffic Accidents ETL" project demonstrates a robust data pipeline, integrating advanced tools like Apache Airflow, PostgreSQL, and Power BI. By processing the Kaggle traffic accident dataset and enriching it with OpenStreetMap data, the project provides deep insights into accident patterns, infrastructure impacts, and risk zones, supporting data-driven strategies for improving road safety and urban planning.

