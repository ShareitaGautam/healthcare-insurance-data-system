from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max

spark = SparkSession.builder \
    .appName("Use Case 04 - Hospital Serving Most Patients") \
    .getOrCreate()

input_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_patients"

df = spark.read.csv(input_path, header=True, inferSchema=True)

print("===== PATIENTS SCHEMA =====")
df.printSchema()

print("===== SAMPLE DATA =====")
df.show(5, truncate=False)

# Count patients per hospital
grouped_df = df.groupBy("hospital_id") \
    .agg(count("patient_id").alias("total_patients"))

print("===== ALL HOSPITAL COUNTS =====")
grouped_df.orderBy(col("total_patients").desc()).show(truncate=False)

# Find maximum patient count
max_count = grouped_df.agg(
    spark_max("total_patients").alias("max_patients")
).collect()[0]["max_patients"]

print(f"Maximum patient count: {max_count}")

# Filter only hospitals with maximum patients
result_df = grouped_df.filter(col("total_patients") == max_count)

print("===== FINAL RESULT: HOSPITAL(S) SERVING MOST PATIENTS =====")
result_df.show(truncate=False)

output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_04_hospital_most_patients"

result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()