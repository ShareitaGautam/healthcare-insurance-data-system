from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, months_between, floor

spark = SparkSession.builder \
    .appName("Use Case 02 - Subscribers Age Less Than 30 With Subgroup") \
    .getOrCreate()

subscriber_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_subscriber"
df = spark.read.csv(subscriber_path, header=True, inferSchema=True)

print("===== SUBSCRIBER SCHEMA =====")
df.printSchema()

print("===== SAMPLE DATA =====")
df.show(5, truncate=False)

# Calculate age using exact column name from your dataset
df = df.withColumn(
    "age",
    floor(months_between(current_date(), col("Birth_date")) / 12)
)

# Filter subscribers age < 30 and having any subgroup
result_df = df.filter(
    (col("age") < 30) &
    (col("Subgrp_id").isNotNull()) &
    (col("Subgrp_id") != "NA")
)

print("===== RESULT: SUBSCRIBERS AGE < 30 WITH SUBGROUP =====")
result_df.show(truncate=False)

output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_02_subscribers_age_lt_30_subgroup"
result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()