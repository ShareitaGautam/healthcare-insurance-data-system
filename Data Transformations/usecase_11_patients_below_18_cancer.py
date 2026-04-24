from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, months_between, floor, lower

spark = SparkSession.builder \
    .appName("Use Case 11 - Patients Below 18 Admitted For Cancer") \
    .getOrCreate()

patients_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_patients"

df = spark.read.csv(patients_path, header=True, inferSchema=True)

print("===== PATIENTS SCHEMA =====")
df.printSchema()

print("===== SAMPLE DATA =====")
df.show(5, truncate=False)

# Calculate age
df = df.withColumn(
    "age",
    floor(months_between(current_date(), col("patient_birth_date")) / 12)
)

# Filter age < 18 and disease contains 'cancer'
result_df = df.filter(
    (col("age") < 18) &
    (lower(col("disease_name")).contains("cancer"))
).select(
    "patient_id",
    "patient_name",
    "patient_gender",
    "patient_birth_date",
    "disease_name",
    "city",
    "hospital_id",
    "age"
)

print("===== FINAL RESULT: PATIENTS BELOW 18 WITH CANCER =====")
result_df.show(truncate=False)

output_path = "s3a://healthcare-insurance-data-platform/output-data/usecase_11_patients_below_18_cancer"

result_df.write.mode("overwrite").csv(output_path, header=True)

print(f"Use case result written to: {output_path}")

spark.stop()
