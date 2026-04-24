from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max

spark = SparkSession.builder \
    .appName("Use Case 05 - Subgroup Most Subscriptions") \
    .getOrCreate()

input_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_subscriber"

df = spark.read.csv(input_path, header=True, inferSchema=True)

print("===== SUBSCRIBER SCHEMA =====")
df.printSchema()

print("===== SAMPLE DATA =====")
df.show(5, truncate=False)

# Exclude NA subgroup values
filtered_df = df.filter(
    (col("Subgrp_id").isNotNull()) &
    (col("Subgrp_id") != "NA")
)

# Count subscriptions per subgroup
grouped_df = filtered_df.groupBy("Subgrp_id") \
    .agg(count("*").alias("total_subscriptions"))

print("===== ALL SUBGROUP COUNTS =====")
grouped_df.orderBy(col("total_subscriptions").desc()).show(truncate=False)

# Find maximum subscription count
max_count = grouped_df.agg(
    spark_max("total_subscriptions").alias("max_subscriptions")
).collect()[0]["max_subscriptions"]

print(f"Maximum subscription count: {max_count}")

# Filter only subgroup(s) with maximum subscriptions
result_df = grouped_df.filter(col("total_subscriptions") == max_count)

print("===== FINAL RESULT: SUBGROUP(S) WITH MOST SUBSCRIPTIONS =====")
result_df.show(truncate=False)

output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_05_subgroup_most_subscriptions"

result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()