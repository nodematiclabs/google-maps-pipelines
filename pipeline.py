from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
import requests
import json
from pyspark.sql.types import BooleanType

# Initialize Spark Session
spark = SparkSession.builder.appName("AddressBusinessCheck").getOrCreate()

# Read data from the tree_census_2015 table
df = spark.read.format("bigquery") \
    .option("table", "bigquery-public-data.new_york_trees.tree_census_2015") \
    .load() \
    .select("address", "zipcode", "zip_city") \
    .limit(100)

# Define a function to call the API
def is_business(address, zipcode, zip_city):
    api_url = "https://addressvalidation.googleapis.com/v1:validateAddress?key=YOUR_KEY"
    payload = {
        "address": {
            "regionCode": "US",
            "locality": zip_city,
            "addressLines": [
                address,
                f"{zip_city}, NY, {zipcode}"
            ]
        }
    }
    response = requests.post(api_url, json=payload)
    if response.status_code == 200:
        return response.json().get('result', {}).get('metadata', {}).get('business', False)
    else:
        return False

# Register UDF
is_business_udf = udf(is_business, BooleanType())

# Apply UDF to DataFrame
df = df.withColumn("is_business", is_business_udf(col("address"), col("zipcode"), col("zip_city")))

# Aggregate Data
business_count = df.filter(col("is_business") == True).count()
non_business_count = df.filter(col("is_business") == False).count()

# Output Results
print(f"Business Address Count: {business_count}")
print(f"Non-Business Address Count: {non_business_count}")

# Stop Spark Session
spark.stop()
