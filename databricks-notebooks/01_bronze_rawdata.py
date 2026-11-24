from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = dbutils.secrets.get(scope="hospital-analytics", key="event-hubs-namespace")
event_hub_name = dbutils.secrets.get(scope="hospital-analytics", key="event-hub-name")
event_hub_conn_str = dbutils.secrets.get(scope="hospital-analytics", key="event-hubs-connection-string")


kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}
#Read from eventhub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

#ADLS configuration 
spark.conf.set(
  f"fs.azure.account.key.{dbutils.secrets.get(scope='hospital-analytics', key='storage-account-name')}.dfs.core.windows.net",
  dbutils.secrets.get(scope="hospital-analytics", key="storage-account-key")
)

storage_account_name = dbutils.secrets.get(scope="hospital-analytics", key="storage-account-name")
storage_container = dbutils.secrets.get(scope="hospital-analytics", key="storage-container")
bronze_path = f"abfss://{storage_container}@{storage_account_name}.core.windows.net/bronze"

#Write stream to bronze
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/mnt/bronze/_checkpoints/patient_flow")
    .start(bronze_path)
)