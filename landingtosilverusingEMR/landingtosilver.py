# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 08:19:16 2024

@author: spattanshett
"""
# Import Libraries
import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, min as min_col

# Initialize SparkSession
spark = SparkSession.builder.appName("From-landing-to-silver").getOrCreate()

# Initialize Boto3 client
s3_client = boto3.client('s3')


# S3 input and output paths
source_bucket = "landingzone48"
source_s3_path = "s3://landingzone48/source_data/"
archived_s3_path = "s3://landingzone48/archived/"
failed_s3_path = "s3://landingzone48/failedfiles/"

destination_s3_path_categories = "s3://capstonedatags/silver-layer/dim_categories/"
destination_s3_path_products = "s3://capstonedatags/silver-layer/dim_products/"
destination_s3_path_users = "s3://capstonedatags/silver-layer/dim_users/"
destination_s3_path_events = "s3://capstonedatags/silver-layer/fact_events/"

# Read the CSV file from the source S3 bucket into a PySpark DataFrame
try:
    # List the files in source_data/
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(source_bucket)
    files = bucket.objects.filter(Prefix='source_data/')
    
    # Process each file in the source_data/ directory
    for file in files:
        file_key = file.key
        if file_key.endswith('/'):  # Skip folder entries
            continue
        file_path = f"s3://{source_bucket}/{file_key}"

        # Read the file
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
        # Show the DataFrame
        df.show()
        
        # Convert the event_time column from UTC to IST (Indian Standard Time)
        df = df.withColumn("event_time_IST", from_utc_timestamp("event_time", "Asia/Kolkata"))
        df = df.drop("event_time")
        df = df.withColumnRenamed("event_time_IST", "event_time")


        # Select category-related columns and drop duplicates
        categories = df.select("category_id", "category_code").dropDuplicates()

        # Fill missing values in category_code with 'UnknownCategory'
        categories = categories.fillna({"category_code": "UnknownCategory"})

        # Show the resulting DataFrames
        print("Categories DataFrame:")
        categories.show()

        # Select product-related columns and drop duplicates
        products = df.select("product_id", "brand").dropDuplicates()

        # Fill missing values in brand with 'UnknownBrand'
        products = products.fillna({"brand": "UnknownBrand"})

        print("Products DataFrame:")
        products.show()

        # Drop the columns category_code and brand
        df = df.drop("category_code","brand")

        users = df.groupBy("user_id").agg(min_col("event_time").alias("first_transaction_time"))

        # Show the resulting DataFrame with first transaction time
        print("Users DataFrame with first transaction time:")
        users.show()

        df.show()
        # Write the results back to S3 in CSV format
        categories.write.mode("append").csv(destination_s3_path_categories)
        products.write.mode("append").csv(destination_s3_path_products)
        users.write.mode("append").csv(destination_s3_path_users)
        df.write.mode("append").csv(destination_s3_path_events)
        
        # If processing is successful, move the file to the archived folder
        archive_key = file_key.replace('source_data/', 'archived/')
        s3_client.copy_object(
            Bucket=source_bucket,
            CopySource={'Bucket': source_bucket, 'Key': file_key},
            Key=archive_key
        )
        s3_client.delete_object(Bucket=source_bucket, Key=file_key)
        print(f"Successfully processed and moved {file_key} to {archive_key}")
        
except Exception as e:
    # If processing fails, move the file to the failedfiles folder
    failed_key = file_key.replace('source_data/', 'failedfiles/')
    s3_client.copy_object(
        Bucket=source_bucket,
        CopySource={'Bucket': source_bucket, 'Key': file_key},
        Key=failed_key
    )
    s3_client.delete_object(Bucket=source_bucket, Key=file_key)
    print(f"Failed to process {file_key}. Moved to {failed_key}. Error: {e}")

finally:
    # Stop the Spark session
    spark.stop()

