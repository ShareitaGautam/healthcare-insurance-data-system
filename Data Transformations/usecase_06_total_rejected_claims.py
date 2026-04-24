from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder \
    .appName("Use Case 06 - Total Rejected Claims") \
    .getOrCreate()

input_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_claims"

df = spark.read.json(input_path)

print("===== CLAIMS SCHEMA =====")
df.printSchema()

print("===== SAMPLE DATA =====")
df.show(5, truncate=False)

result_df = df.filter(col("claim_status") == "REJECTED") \
    .agg(count("*").alias("total_rejected_claims"))

print("===== FINAL RESULT: TOTAL REJECTED CLAIMS =====")
result_df.show(truncate=False)

output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_06_total_rejected_claims"

result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()