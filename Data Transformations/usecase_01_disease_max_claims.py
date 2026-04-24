from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max

spark = SparkSession.builder \
    .appName("Use Case 01 - Disease With Maximum Claims") \
    .getOrCreate()

input_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_claims"

df = spark.read.json(input_path)

print("===== CLEANED CLAIMS SCHEMA =====")
df.printSchema()

print("===== SAMPLE DATA =====")
df.show(5, truncate=False)

# Count claims by disease
grouped_df = df.groupBy("disease_name") \
    .agg(count("*").alias("total_claims"))

print("===== ALL DISEASE CLAIM COUNTS =====")
grouped_df.orderBy(col("total_claims").desc()).show(truncate=False)

# Find maximum claim count
max_count = grouped_df.agg(
    spark_max("total_claims").alias("max_claims")
).collect()[0]["max_claims"]

print(f"Maximum claims count: {max_count}")

# Filter only diseases having maximum claims
result_df = grouped_df.filter(col("total_claims") == max_count)

print("===== FINAL RESULT: DISEASE(S) WITH MAXIMUM CLAIMS =====")
result_df.show(truncate=False)

# Save final result
output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_01_disease_max_claims"

result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()