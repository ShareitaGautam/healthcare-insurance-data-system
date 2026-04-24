from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, to_date

spark = SparkSession.builder \
    .appName("Clean Subscriber Data") \
    .getOrCreate()

input_path = "s3a://healthcare-insurance-data-platform/input-data/subscriber.csv"

df = spark.read.csv(input_path, header=True, inferSchema=True)

print("===== ORIGINAL SCHEMA =====")
df.printSchema()

print("===== ORIGINAL DATA =====")
df.show(5, truncate=False)

# 1. Check duplicates
print("Total rows before dropDuplicates:", df.count())
df = df.dropDuplicates()
print("Total rows after dropDuplicates:", df.count())

# 2. Trim string columns
for column_name, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(column_name, trim(col(column_name)))

# 3. Convert empty strings / NaN to NULL
for column_name, dtype in df.dtypes:
    if dtype == "string":
        df = df.withColumn(
            column_name,
            when((col(column_name) == "") | (col(column_name) == "NaN"), None)
            .otherwise(col(column_name))
        )

# 4. Count NULL values
print("===== NULL COUNTS BEFORE REPLACEMENT =====")
for c in df.columns:
    null_count = df.filter(col(c).isNull()).count()
    print(f"{c}: {null_count}")

# 5. Convert date columns for Redshift compatibility
if "Birth_date" in df.columns:
    df = df.withColumn("Birth_date", to_date(col("Birth_date")))

if "eff_date" in df.columns:
    df = df.withColumn("eff_date", to_date(col("eff_date")))

if "term_date" in df.columns:
    df = df.withColumn("term_date", to_date(col("term_date")))

# 6. Replace NULL with 'NA' for string columns
string_columns = [name for name, dtype in df.dtypes if dtype == "string"]
df = df.fillna("NA", subset=string_columns)

print("===== CLEANED SCHEMA =====")
df.printSchema()

print("===== CLEANED DATA =====")
df.show(10, truncate=False)

print("===== NULL COUNTS AFTER REPLACEMENT =====")
for c in df.columns:
    null_count = df.filter(col(c).isNull()).count()
    print(f"{c}: {null_count}")

# 7. Save cleaned subscriber data
output_path = "s3a://healthcare-insurance-data-platform/output-data/cleaned_subscriber"

df.write.mode("overwrite").csv(output_path, header=True)

print(f"Cleaned subscriber data written to: {output_path}")

spark.stop()