from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max

spark = SparkSession.builder \
    .appName("Use Case 07 - City With Most Claims") \
    .getOrCreate()

claims_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_claims"
patients_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_patients"

claims_df = spark.read.json(claims_path)
patients_df = spark.read.csv(patients_path, header=True, inferSchema=True)

print("===== JOINING DATA =====")
joined_df = claims_df.join(
    patients_df,
    claims_df["patient_id"] == patients_df["patient_id"],
    "inner"
)

joined_df.show(5)

# Count claims per city
grouped_df = joined_df.groupBy("city") \
    .agg(count("*").alias("total_claims"))

print("===== ALL CITY CLAIM COUNTS =====")
grouped_df.orderBy(col("total_claims").desc()).show(truncate=False)

# Find max
max_count = grouped_df.agg(
    spark_max("total_claims").alias("max_claims")
).collect()[0]["max_claims"]

print(f"Maximum claims count: {max_count}")

# Filter only max city
result_df = grouped_df.filter(col("total_claims") == max_count)

print("===== FINAL RESULT: CITY WITH MOST CLAIMS =====")
result_df.show(truncate=False)

output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_07_city_most_claims"

result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()