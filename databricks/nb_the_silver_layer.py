# Databricks notebook source
# MAGIC %run "./nb_config"

# COMMAND ----------


logger = logging.getLogger("silver ingestion")
try:
    bronze_df = spark.readStream.format("delta").load(BRONZE_TABLE_PATH)

    silver_df = (
        bronze_df
            .withColumn("update_ts", to_timestamp("update_ts"))
            .filter(col("customer_id").isNotNull())
            .filter(col("operation").isin("INSERT", "UPDATE"))
    )

    # Handle late events using watermark
    silver_df = silver_df.withWatermark("update_ts", f"{WATERMARKING} day")

    # Deduplicate CDC events
    window_spec = Window.partitionBy("customer_id", "update_ts") \
                        .orderBy(col("ingestion_time").desc())

    dedup_df = (
        silver_df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
    )

    # Write stream to Silver
    query = (
        dedup_df.writeStream
            .format("delta")
            .option("checkpointLocation", SILVER_CHECKPOINT)
            .outputMode("append")
            .start(SILVER_TABLE_PATH)
    )

    query.awaitTermination()

except Exception as e:
    logger.error("Streaming failed....")
    raise e