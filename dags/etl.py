from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json

## Define the dag
with DAG(
    dag_id = 'alpha_vantage_daily_etl',
    start_date=datetime(2026,1,1),
    schedule = '@daily',
    catchup = False
) as dag:
    
    ## Step 1 : Create the table if it does not exist
    @task
    def create_table():
        ## initalize the postgres hook
        postgres_hook = PostgresHook(postgres_conn_id = "my_postgres_connection")

        ## SQL create th table
        
        query_create_table ="""
        CREATE TABLE IF NOT EXISTS time_series_data (
            symbol        VARCHAR(10) NOT NULL,
            trading_date  DATE        NOT NULL,
            open          NUMERIC(12,4) NOT NULL,
            high          NUMERIC(12,4) NOT NULL,
            low           NUMERIC(12,4) NOT NULL,
            close         NUMERIC(12,4) NOT NULL,
            volume        BIGINT      NOT NULL,
            PRIMARY KEY (symbol, trading_date)
        );
        """

        ## EXECUTE the table creation query
        postgres_hook.run(query_create_table)
    ## Step 2: Extract Alpha Vantage daily stock prices
    extract_time_series = HttpOperator(
        task_id='extract_finance',
        http_conn_id='alphavantage_api',
        endpoint='query',
        method='GET',
        # In this provider version, `data` is used to supply request parameters.
        data={
            "function": "TIME_SERIES_DAILY",
            "symbol": "IBM",
            "outputsize": "compact",
            "apikey": "{{ conn.alphavantage_api.extra_dejson.api_key }}",
        },
        response_filter=lambda response: response.json(),
        log_response=True,
    )

    ## Step 3: Transform the data (pick the information that o need to save)
    @task
    def transform_time_series_data(response):
        # Alpha Vantage sometimes returns non-standard payloads (rate limits, invalid API key, etc.)
        # Instead of crashing with KeyError, detect and raise a clear error.

        # If the operator ever returns a JSON string, parse it.
        if isinstance(response, str):
            try:
                response = json.loads(response)
            except Exception:
                raise ValueError(f"Unexpected non-JSON response from API: {response[:200]}")

        # Common Alpha Vantage error/throttle keys
        if isinstance(response, dict):
            if "Note" in response:
                raise ValueError(f"Alpha Vantage rate limit hit: {response.get('Note')}")
            if "Information" in response:
                raise ValueError(f"Alpha Vantage info/error: {response.get('Information')}")
            if "Error Message" in response:
                raise ValueError(f"Alpha Vantage error: {response.get('Error Message')}")

        # Validate expected keys
        if not isinstance(response, dict) or "Meta Data" not in response or "Time Series (Daily)" not in response:
            raise ValueError(
                "Alpha Vantage response missing expected keys. "
                f"Top-level keys received: {list(response.keys()) if isinstance(response, dict) else type(response)}"
            )

        symbol = response["Meta Data"].get("2. Symbol")
        if not symbol:
            raise ValueError(f"Could not find symbol in Meta Data: {response['Meta Data']}")

        time_series = response["Time Series (Daily)"]
        rows = []
        for trading_date, values in time_series.items():
            rows.append({
                "symbol": symbol,
                "trading_date": trading_date,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"]),
            })

        return rows
    ## Step 4 : Load the data into postgres SQL
    @task
    def load_data_to_postgres(time_series_rows):
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        insert_query = """
        INSERT INTO time_series_data (symbol, trading_date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, trading_date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close= EXCLUDED.close,
            volume = EXCLUDED.volume;
        """

        for row in time_series_rows:
            postgres_hook.run(
                insert_query,
                parameters=(
                    row["symbol"],
                    row["trading_date"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                ),
            )

    ## step 5: Verify the data DBViewer

    ## step 6: Define the task dependencies
    ## Extract
    create_table() >> extract_time_series
    api_response = extract_time_series.output
    ## Transform
    transformed_data = transform_time_series_data(api_response)
    ## Load
    load_data_to_postgres(transformed_data)