import pymongo
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import os

# Set up Google Cloud Storage and BigQuery clients (ensure you have proper authentication)
storage_client = storage.Client()
bigquery_client = bigquery.Client()

def get_connection():
    db_name = None
    collection = None
    try:
        # Connect to the MongoDB server
        connection_url = pymongo.MongoClient('mongodb+srv://Sal:admin123@sal.94xhv.mongodb.net/')
        
        # Access the specific database
        db_name = connection_url["etl"]
        
        # Access the specific collection (replace 'your_collection_name' with the actual collection name)
        collection = db_name["data"]
        
        print(f"Connected to collection: {collection}")
        
        # Fetching all documents in the collection
        data = collection.find()  
        for document in data:
            print(document)  # Print each document
        
    except Exception as exception:
        print(f"An error occurred: {exception}")
    
    return db_name, collection

def extract_transform_data():
    # Extract Data
    db_name, collection = get_connection()
    data_cursor = collection.find()  # Fetch all documents

    # List to store transformed data
    transformed_data = []

    # Loop through the documents and transform the data
    for document in data_cursor:
        transformed_doc = {
            "order_id": document.get("order_id"),
            "user_id": str(document.get("user_id", {}).get("$oid", "")),
            "product_id": str(document.get("product_id", {}).get("$oid", "")),
            "form_id": str(document.get("form_id", {}).get("$oid", "")),
            "status": document.get("status"),
            "total_product_price": document.get("total_product_price"),
            "total_price": document.get("total_price"),
            "created_at": document.get("created_at", {}).get("$date", ""),
            "updated_at": document.get("updated_at", {}).get("$date", ""),
            "shipping_cost": document.get("shipping", {}).get("cost", 0),
            "shipping_mode": document.get("shipping", {}).get("mode", ""),
            "customer_name": document.get("customer_data", {}).get("name", ""),
            "customer_email": document.get("customer_data", {}).get("email", ""),
            "customer_phone": document.get("customer_data", {}).get("phone", ""),
            "product_name": document.get("product_data", {}).get("name", ""),
            "product_slug": document.get("product_data", {}).get("slug", ""),
            "product_code": document.get("product_data", {}).get("code", ""),
            "product_price": document.get("product_data", {}).get("price", 0),
            "cogs": document.get("product_data", {}).get("cogs", 0),
        }
        
        # Adding nested fields transformation for `product_data`
        if 'product_data' in document:
            product_data = document["product_data"]
            transformed_doc.update({
                "product_sale_price": product_data.get("sale_price", {}).get("price", 0),
                "product_picture": product_data.get("picture", ""),
                "product_type": product_data.get("type", ""),
            })
        
        transformed_data.append(transformed_doc)
    
    return transformed_data

# Function to save to Parquet and upload to GCS
def save_to_parquet(data, bucket_name, file_name):
    df = pd.DataFrame(data)
    parquet_file_path = f"/tmp/{file_name}.parquet"
    df.to_parquet(parquet_file_path, engine='pyarrow')

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name + ".parquet")
    blob.upload_from_filename(parquet_file_path)
    print(f"Data saved to Cloud Storage: {file_name}.parquet")

# Function to load data to BigQuery
def load_to_bigquery(bucket_name, file_name, dataset_id, table_id):
    uri = f"gs://{bucket_name}/{file_name}.parquet"
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
    load_job = bigquery_client.load_table_from_uri(uri, f"{dataset_id}.{table_id}", job_config=job_config)
    load_job.result()
    print(f"Data loaded into BigQuery: {dataset_id}.{table_id}")
