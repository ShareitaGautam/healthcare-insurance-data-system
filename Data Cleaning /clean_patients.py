from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, upper

spark = SparkSession.builder \
    .appName("Clean Patients Data") \
    .getOrCreate()

input_path = "s3a://healthcare-insurance-data-platform/input-data/Patient_records.csv"

df = spark.read.csv(input_path, header=True, inferSchema=True)

print("===== ORIGINAL SCHEMA =====")
df.printSchema()

print("===== ORIGINAL DATA =====")
df.show(5, truncate=False)

# -----------------------------
# 1. Remove duplicates
# -----------------------------
print("Before duplicates:", df.count())
df = df.dropDuplicates()
print("After duplicates:", df.count())

# -----------------------------
# 2. Trim string columns
# -----------------------------
for column_name, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(column_name, trim(col(column_name)))

# -----------------------------
# 3. Convert empty/NaN to NULL
# -----------------------------
for column_name, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(
            column_name,
            when((col(column_name) == "") | (col(column_name) == "NaN"), None)
            .otherwise(col(column_name))
        )

# -----------------------------
# 4. Count NULLS
# -----------------------------
print("===== NULL COUNTS BEFORE =====")
for c in df.columns:
    print(c, df.filter(col(c).isNull()).count())

# -----------------------------
# 5. Standardize columns
# -----------------------------
# Adjust names based on your schema if needed
df = df.withColumnRenamed("Patient_id", "patient_id") \
       .withColumnRenamed("Patient_name", "patient_name")
from pyspark.sql.functions import upper
# Standardize gender
if "patient_gender" in df.columns:
    df = df.withColumn("patient_gender", upper(col("patient_gender")))

# -----------------------------
# 6. Replace NULL with 'NA'
# -----------------------------
string_columns = [name for name, dtype in df.dtypes if dtype == "string"]
df = df.fillna("NA", subset=string_columns)

# -----------------------------
# 7. Final check
# -----------------------------
print("===== CLEANED DATA =====")
df.show(10, truncate=False)

print("===== NULL COUNTS AFTER =====")
for c in df.columns:
    print(c, df.filter(col(c).isNull()).count())

# -----------------------------
# 8. Save cleaned data
# -----------------------------
output_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_patients"

df.write.mode("overwrite").csv(output_path, header=True)

print(f"Cleaned patients data written to: {output_path}")

spark.stop()