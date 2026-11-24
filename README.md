# Hospital Patient Dataflow Analytics

An end-to-end Azure analytics project that generates hospital patient-flow events, streams them through Azure Event Hubs, cleans and transforms the data with Azure Databricks, and exposes analytics through Azure Synapse SQL Pool and Power BI.

## Architecture

1. The Python simulator generates sample patient-flow events.
2. Events are sent to Azure Event Hubs using the Kafka protocol.
3. Databricks bronze, silver, and gold notebooks read, clean, and transform the events.
4. Azure Synapse external tables and views expose the transformed data.
5. Power BI consumes the analytical views for reporting.

## Repository layout

- `simulator/patient_flow_generator.py` - Generates and publishes patient events.
- `test_kafka.py` - Tests the Event Hubs/Kafka connection.
- `databricks-notebooks/01_bronze_rawdata.py` - Streams raw events into Delta.
- `databricks-notebooks/02_silver_cleandata.py` - Cleans and validates streamed events.
- `databricks-notebooks/03_gold_transform.py` - Creates dimensions and fact tables.
- `sqlpool-quries/` - Synapse SQL setup and analytics views.
- `powerbi/Hospital_Dashboard.pbix` - Power BI dashboard.
- `.env.example` - Example environment configuration.

## Local setup

Create and activate a Python virtual environment:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Copy `.env.example` to `.env`, then set the Azure Event Hubs values:

```dotenv
EVENT_HUBS_NAMESPACE=<event-hubs-namespace>.servicebus.windows.net
EVENT_HUB_NAME=<event-hub-name>
EVENT_HUB_CONNECTION_STRING=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<key-name>;SharedAccessKey=<shared-access-key>
```

`.env` is excluded by `.gitignore` and must not be committed.

## Running the simulator

Start the Event Hubs connection test:

```powershell
python test_kafka.py
```

Start continuous event generation:

```powershell
python simulator/patient_flow_generator.py
```

## Databricks configuration

Create a Databricks secret scope named `hospital-analytics` and add these secrets:

| Key | Purpose |
|---|---|
| `event-hubs-namespace` | Event Hubs namespace hostname |
| `event-hub-name` | Event Hub name |
| `event-hubs-connection-string` | Kafka-compatible Event Hubs connection string |
| `storage-account-name` | ADLS storage account name |
| `storage-account-key` | ADLS storage account key |
| `storage-container` | ADLS container name |

For example:

```python
dbutils.secrets.get(scope="hospital-analytics", key="event-hubs-connection-string")
```

Import the notebooks in numeric order:

1. `01_bronze_rawdata.py`
2. `02_silver_cleandata.py`
3. `03_gold_transform.py`

## Azure Synapse SQL

Run the SQL scripts in `sqlpool-quries/` in your Synapse dedicated SQL pool:

1. `SQL_pool_quries.sql` creates the data source, file format, external tables, and database objects.
2. `SQL_views_DDL.sql` creates the reporting views.

Use Azure-managed identity or another approved credential mechanism for storage access. Do not place passwords or keys directly in SQL or commit them to Git.

## Power BI

Open `powerbi/Hospital_Dashboard.pbix` in Power BI Desktop and configure the appropriate Synapse SQL endpoint and credentials.
