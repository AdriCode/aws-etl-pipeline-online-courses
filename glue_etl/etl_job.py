import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## ============================================================
## SECTION 1 — READ RAW DATA FROM S3
## ============================================================

udemy_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .csv("s3://online-courses-raw-ashwath/udemy/")

coursera_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .csv("s3://online-courses-raw-ashwath/coursera/")

## ============================================================
## SECTION 2 — CLEAN UDEMY DATA
## (Actual columns: id, title, is_paid, price, headline,
##  num_subscribers, avg_rating, num_reviews, num_comments,
##  num_lectures, content_length_min, published_time,
##  last_update_date, category, subcategory, topic, language,
##  course_url, instructor_name, instructor_url)
## ============================================================

udemy_clean = udemy_df \
    .dropDuplicates() \
    .dropna(subset=["title"]) \
    .withColumn("price", F.col("price").cast(DoubleType())) \
    .withColumn("num_subscribers", F.col("num_subscribers").cast(LongType())) \
    .withColumn("num_reviews", F.col("num_reviews").cast(LongType())) \
    .withColumn("num_lectures", F.col("num_lectures").cast(IntegerType())) \
    .withColumn("content_duration", F.col("content_length_min").cast(DoubleType())) \
    .withColumn("avg_rating", F.col("avg_rating").cast(DoubleType())) \
    .withColumn("price", F.when(F.col("price").isNull(), 0.0).otherwise(F.col("price"))) \
    .withColumn("price_category",
        F.when(F.col("price") == 0, "Free")
         .when(F.col("price") < 20, "Low")
         .when(F.col("price") < 50, "Medium")
         .otherwise("High")) \
    .withColumn("published_year",
        F.when(F.col("published_time").isNotNull(),
            F.year(F.to_timestamp(F.col("published_time")))).otherwise(None)) \
    .withColumn("published_month",
        F.when(F.col("published_time").isNotNull(),
            F.month(F.to_timestamp(F.col("published_time")))).otherwise(None)) \
    .withColumn("platform", F.lit("Udemy")) \
    .withColumn("certificate_type",
        F.when(F.col("is_paid") == True, "Paid").otherwise("Free")) \
    .withColumn("course_title", F.col("title")) \
    .withColumn("subject", F.col("category")) \
    .withColumn("level", F.lit("Unknown")) \
    .withColumn("organization", F.col("instructor_name")) \
    .withColumn("course_rating", F.col("avg_rating"))

## ============================================================
## SECTION 3 — CLEAN COURSERA DATA
## ============================================================

def parse_enrollment(val):
    try:
        if val is None:
            return None
        val = str(val).strip().lower()
        val = val.replace(",", "").replace(" ", "")
        # remove any non-numeric/non-letter characters except decimal
        import re
        # handle ranges like "1k-5k", take the upper bound
        if "-" in val:
            val = val.split("-")[-1]
        if "m+" in val or val.endswith("m"):
            num = re.sub(r'[^0-9.]', '', val)
            return int(float(num) * 1000000) if num else None
        elif "k+" in val or val.endswith("k"):
            num = re.sub(r'[^0-9.]', '', val)
            return int(float(num) * 1000) if num else None
        else:
            num = re.sub(r'[^0-9.]', '', val)
            return int(float(num)) if num else None
    except:
        return None

parse_enrollment_udf = F.udf(parse_enrollment, LongType())

coursera_clean = coursera_df \
    .dropDuplicates() \
    .dropna(subset=["title"]) \
    .withColumn("course_title", F.col("title")) \
    .withColumn("course_rating",
        F.when(F.col("rating").cast(DoubleType()).isNotNull(),
            F.col("rating").cast(DoubleType())).otherwise(None)) \
    .withColumn("num_subscribers",
        F.when(F.col("enrolled").isNotNull(),
            parse_enrollment_udf(F.col("enrolled").cast(StringType()))
        ).otherwise(None)) \
    .withColumn("num_reviews",
        F.when(F.col("num_reviews").cast(LongType()).isNotNull(),
            F.col("num_reviews").cast(LongType())).otherwise(None)) \
    .withColumn("price", F.lit(None).cast(DoubleType())) \
    .withColumn("price_category", F.lit("Unknown")) \
    .withColumn("num_lectures", F.lit(None).cast(IntegerType())) \
    .withColumn("content_duration", F.lit(None).cast(DoubleType())) \
    .withColumn("published_year", F.lit(None).cast(IntegerType())) \
    .withColumn("published_month", F.lit(None).cast(IntegerType())) \
    .withColumn("platform", F.lit("Coursera")) \
    .withColumn("subject",
        F.when(F.col("Organization").isNotNull(),
            F.col("Organization")).otherwise("Unknown")) \
    .withColumn("level",
        F.when(F.col("Level").isNull(), "Unknown")
         .otherwise(F.col("Level"))) \
    .withColumn("certificate_type", F.lit("Course")) \
    .withColumn("organization",
        F.when(F.col("Organization").isNotNull(),
            F.col("Organization")).otherwise("Unknown"))

## ============================================================
## SECTION 4 — UNIFY SCHEMA AND JOIN
## ============================================================

unified_columns = [
    "course_title", "platform", "organization", "subject",
    "level", "course_rating", "num_subscribers", "num_reviews",
    "num_lectures", "content_duration", "price", "price_category",
    "certificate_type", "published_year", "published_month"
]

udemy_final = udemy_clean.select(unified_columns)
coursera_final = coursera_clean.select(unified_columns)

joined_df = udemy_final.union(coursera_final)

joined_df = joined_df.withColumn("course_id",
    F.monotonically_increasing_id().cast(LongType()))

## ============================================================
## SECTION 5 — BUILD STAR SCHEMA DIMENSIONS
## ============================================================

fact_df = joined_df.select(
    "course_id", "course_title", "platform", "subject",
    "level", "course_rating", "num_subscribers", "num_reviews",
    "num_lectures", "content_duration", "price",
    "price_category", "certificate_type",
    "published_year", "published_month"
)

dim_platform = joined_df.select("platform").dropDuplicates() \
    .withColumn("platform_id", F.monotonically_increasing_id().cast(LongType()))

dim_category = joined_df.select("subject", "level").dropDuplicates() \
    .withColumn("category_id", F.monotonically_increasing_id().cast(LongType()))

dim_certificate = joined_df.select("certificate_type", "price_category").dropDuplicates() \
    .withColumn("certificate_id", F.monotonically_increasing_id().cast(LongType()))

dim_date = joined_df.select("published_year", "published_month").dropDuplicates() \
    .filter(F.col("published_year").isNotNull()) \
    .withColumn("date_id", F.monotonically_increasing_id().cast(LongType()))

## ============================================================
## SECTION 6 — WRITE OUTPUT TO S3
## ============================================================

OUTPUT_BASE = "s3://online-courses-processed-ashwath"

udemy_final.write.mode("overwrite").option("header", "true") \
    .csv(f"{OUTPUT_BASE}/cleaned/udemy/")

coursera_final.write.mode("overwrite").option("header", "true") \
    .csv(f"{OUTPUT_BASE}/cleaned/coursera/")

joined_df.write.mode("overwrite").option("header", "true") \
    .csv(f"{OUTPUT_BASE}/joined/")

fact_df.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/star-schema/fact/")
dim_platform.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/star-schema/dim-platform/")
dim_category.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/star-schema/dim-category/")
dim_certificate.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/star-schema/dim-certificate/")
dim_date.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/star-schema/dim-date/")

job.commit()
