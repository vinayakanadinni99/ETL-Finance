### Project Overview: Airflow ETL Pipeline with Postgres and API Integration

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline extracts financial time-series data from an external API (Alpha Vantage Daily Stock Prices API), transforms the data into a structured format, and loads it into a PostgreSQL database. The entire workflow is orchestrated by Apache Airflow, which enables scheduling, monitoring, and reliable execution of data pipelines.

The project leverages Docker to run Airflow and Postgres as containerized services, ensuring an isolated, reproducible, and scalable environment. Airflow hooks and operators are used to manage API ingestion and database interactions efficiently.

⸻

#### Key Components of the Project

#### Airflow for Orchestration

	•	Apache Airflow is used to define, schedule, and monitor the complete ETL pipeline.
	•	The workflow is represented as a Directed Acyclic Graph (DAG) consisting of tasks such as:
	•	Database table creation
	•	API data extraction
	•	Data transformation
	•	Data loading
	•	Task dependencies ensure the pipeline executes in the correct sequence.
	•	The project utilizes Airflow’s TaskFlow API (@task), along with HttpOperator and PostgresHook.

⸻

#### PostgreSQL Database

	•	PostgreSQL is used as the persistent data store for transformed stock market data.
	•	Postgres runs inside a Docker container, with data persistence managed through Docker volumes.
	•	The database schema is designed for time-series financial data, capturing:
	•	Stock symbol
	•	Trading date
	•	Open, High, Low, Close prices
	•	Trading volume
	•	Data insertion is handled using UPSERT logic, preventing duplicate records and ensuring consistency.

⸻

#### Alpha Vantage API (Financial Market Data)
	•	The external data source for this project is the Alpha Vantage TIME_SERIES_DAILY API, which provides daily stock market data.
	•	The API returns OHLCV data (Open, High, Low, Close, Volume) for a given stock symbol (for example, IBM).
	•	API authentication is handled securely using Airflow Connections, ensuring that API keys are not hard-coded.
	•	Data is retrieved in JSON format and passed downstream for processing.

⸻

### Objectives of the Project

#### Extract Data
	•	Extract daily stock price data from the Alpha Vantage API on a scheduled basis.

#### Transform Data
	•	Validate and normalize the API response into a relational format.
	•	Convert numeric values into appropriate data types for analytical use.
	•	Implement error handling for scenarios such as:
	•	Missing or invalid API keys
	•	API rate limits
	•	Unexpected response formats

#### Load Data into Postgres
	•	Load the transformed data into PostgreSQL using Airflow’s PostgresHook.
	•	Automatically create the target table if it does not already exist.
	•	Ensure idempotent writes to maintain data integrity across multiple runs.

⸻

### Architecture and Workflow

The ETL pipeline is orchestrated in Airflow using a Directed Acyclic Graph and follows a standard Extract → Transform → Load pattern.

#### Extract
	•	The HttpOperator is used to make HTTP GET requests to the Alpha Vantage API.
	•	API parameters such as function name, stock symbol, output size, and API key are dynamically passed at runtime.
	•	The response is returned in JSON format.

#### Transform
	•	The extracted JSON payload is processed using Airflow’s TaskFlow API.
	•	Relevant fields including stock symbol, trading date, price metrics, and volume are extracted and cleaned.
	•	The transformed output is prepared for insertion into the database.

#### Load
	•	The transformed records are inserted into PostgreSQL using PostgresHook.
	•	A composite primary key ensures one record per stock per trading day.
	•	Existing records are updated when newer data is available.

⸻

### Conclusion

This project demonstrates the design and implementation of a production-style ETL pipeline using industry-standard tools and best practices. It highlights skills in workflow orchestration, API integration, data transformation, and relational database management, making it well-suited for data engineering, analytics, and MLOps portfolios.