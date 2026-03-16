# Databricks notebook source
# MAGIC %run "./nb_config"

# COMMAND ----------

logger = logging.getLogger("bronze ingestion")

def bronze_ingest():

    bronze_query = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.useNotifications", "true")
            .option("cloudFiles.maxFilesPerTrigger", 100)
            .option("cloudFiles.includeExistingFiles", "true")
            .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
            .option("badRecordsPath", f"{BRONZE_TABLE_PATH}/bad_records")
            .load(RAW_SOURCE_PATH)
            .withColumn("ingest_ts", current_timestamp())
            .withColumn("source_file", input_file_name())
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", BRONZE_CHECKPOINT)
            .start(BRONZE_TABLE_PATH)
    )
    return bronze_query

for attempt in range(MAX_RETRIES):

    try:
        logger.info(f"Starting Bronze Ingestion. Attempt {attempt+1}")
        query = bronze_ingest()
        query.awaitTermination()
        logger.info("Bronze Ingestion completed successfully")
        break

    except Exception as e:
        if attempt < MAX_RETRIES - 1:
            logger.info(f"Retrying in {RETRY_WAIT_SECONDS} seconds...")
            time.sleep(RETRY_WAIT_SECONDS)
        else:
            logger.error("Max retries reached. Failing Stream....")
            dbutils.notebook.exit(f"STREAM FAILED: {str(e)}")