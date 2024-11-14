# import os
# import logging
# import pandas as pd
# from airflow import DAG
# from airflow.utils.dates import days_ago
# from datetime import datetime
# from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.operators.python import PythonOperator
# from airflow.utils.task_group import TaskGroup
# from bots.MongoHelper import extract_transform_data

# PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET = os.environ.get("GCP_GCS_BUCKET")

# # File paths for transformed data
# dataset_transform_file = "transformed_data"
# path_to_raw_dataset = '/opt/airflow/dataset/raw'
# path_to_transform_dataset = '/opt/airflow/dataset/transform'

# # Function to fetch and transform the data from MongoDB using MongoHelper
# def extract_transform_data_task():
#     # Extract and transform the data using MongoHelper
#     transformed_data = extract_transform_data()

#     # Convert the transformed data to a DataFrame
#     df = pd.DataFrame(transformed_data)

#     # Save the transformed data to Parquet
#     main_data = df.drop(columns=['product_data', 'customer_data', 'shipping'], errors='ignore')  # Main Data
#     product_data = df[['product_name', 'product_slug', 'product_code', 'product_price', 'cogs']]  # Product Data
#     customer_data = df[['customer_name', 'customer_email', 'customer_phone']]  # Customer Data
#     shipping_data = df[['shipping_cost', 'shipping_mode']]  # Shipping Data

#     # Save to Parquet
#     main_data.to_parquet(f"{path_to_transform_dataset}/{dataset_transform_file}_main.parquet")
#     product_data.to_parquet(f"{path_to_transform_dataset}/{dataset_transform_file}_product_data.parquet")
#     customer_data.to_parquet(f"{path_to_transform_dataset}/{dataset_transform_file}_customer_data.parquet")
#     shipping_data.to_parquet(f"{path_to_transform_dataset}/{dataset_transform_file}_shipping.parquet")

# # Airflow DAG definition
# with DAG(
#     dag_id="etl_mongodb_to_gcs_to_bigquery",
#     start_date=days_ago(1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:
    
#     with TaskGroup('extract_transform_load') as etl_tasks:
        
#         # Task 1: Extract and transform data into separate DataFrames for product_data, customer_data, and shipping
#         transform_raw_data = PythonOperator(
#             task_id='transform_raw_data',
#             python_callable=extract_transform_data_task
#         )
        
#         # Task 2: Upload main data, product data, customer data, and shipping data to GCS
#         upload_main_data_to_gcs = LocalFilesystemToGCSOperator(
#             task_id='upload_main_data_to_gcs',
#             src=f"{path_to_transform_dataset}/{dataset_transform_file}_main.parquet",
#             dst=f"transform/{dataset_transform_file}_main.parquet",
#             bucket=BUCKET
#         )
        
#         upload_product_data_to_gcs = LocalFilesystemToGCSOperator(
#             task_id='upload_product_data_to_gcs',
#             src=f"{path_to_transform_dataset}/{dataset_transform_file}_product_data.parquet",
#             dst=f"transform/{dataset_transform_file}_product_data.parquet",
#             bucket=BUCKET
#         )
        
#         upload_customer_data_to_gcs = LocalFilesystemToGCSOperator(
#             task_id='upload_customer_data_to_gcs',
#             src=f"{path_to_transform_dataset}/{dataset_transform_file}_customer_data.parquet",
#             dst=f"transform/{dataset_transform_file}_customer_data.parquet",
#             bucket=BUCKET
#         )
        
#         upload_shipping_data_to_gcs = LocalFilesystemToGCSOperator(
#             task_id='upload_shipping_data_to_gcs',
#             src=f"{path_to_transform_dataset}/{dataset_transform_file}_shipping.parquet",
#             dst=f"transform/{dataset_transform_file}_shipping.parquet",
#             bucket=BUCKET
#         )

#         # Task 3: Load the separate Parquet files to BigQuery
#         load_main_data_to_bigquery = GCSToBigQueryOperator(
#             task_id='load_main_data_to_bigquery',
#             bucket=BUCKET,
#             source_format="PARQUET",
#             source_objects=f"transform/{dataset_transform_file}_main.parquet",
#             destination_project_dataset_table=f"{PROJECT_ID}.demo_etl.transformed_main_data",
#             write_disposition='WRITE_TRUNCATE'
#         )

#         load_product_data_to_bigquery = GCSToBigQueryOperator(
#             task_id='load_product_data_to_bigquery',
#             bucket=BUCKET,
#             source_format="PARQUET",
#             source_objects=f"transform/{dataset_transform_file}_product_data.parquet",
#             destination_project_dataset_table=f"{PROJECT_ID}.demo_etl.transformed_product_data",
#             write_disposition='WRITE_TRUNCATE'
#         )

#         load_customer_data_to_bigquery = GCSToBigQueryOperator(
#             task_id='load_customer_data_to_bigquery',
#             bucket=BUCKET,
#             source_format="PARQUET",
#             source_objects=f"transform/{dataset_transform_file}_customer_data.parquet",
#             destination_project_dataset_table=f"{PROJECT_ID}.demo_etl.transformed_customer_data",
#             write_disposition='WRITE_TRUNCATE'
#         )

#         load_shipping_data_to_bigquery = GCSToBigQueryOperator(
#             task_id='load_shipping_data_to_bigquery',
#             bucket=BUCKET,
#             source_format="PARQUET",
#             source_objects=f"transform/{dataset_transform_file}_shipping.parquet",
#             destination_project_dataset_table=f"{PROJECT_ID}.demo_etl.transformed_shipping_data",
#             write_disposition='WRITE_TRUNCATE'
#         )

#         # Task dependencies: Extract and transform -> Upload to GCS -> Load to BigQuery
#         transform_raw_data >> [
#             upload_main_data_to_gcs, 
#             upload_product_data_to_gcs, 
#             upload_customer_data_to_gcs, 
#             upload_shipping_data_to_gcs
#         ]
        
#         upload_main_data_to_gcs >> load_main_data_to_bigquery
#         upload_product_data_to_gcs >> load_product_data_to_bigquery
#         upload_customer_data_to_gcs >> load_customer_data_to_bigquery
#         upload_shipping_data_to_gcs >> load_shipping_data_to_bigquery





import os
import logging
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# File paths for transformed data
dataset_transform_file = "transformed_data"
path_to_raw_dataset = '/opt/airflow/dataset/raw'
path_to_transform_dataset = '/opt/airflow/dataset/transform'

# Function to fetch and transform the data from MongoDB using MongoHook
def extract_transform_data_task():
    # Initialize MongoHook
    mongo_hook = MongoHook(conn_id='mongo_default')  # Use the default MongoDB connection ID or set your own

    # Connect to MongoDB and fetch the data from the collection
    collection = mongo_hook.get_collection('etl', 'data')  # Provide database and collection names
    data_cursor = collection.find()

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

    # Convert the transformed data to a DataFrame
    df = pd.DataFrame(transformed_data)

    # Save the transformed data to Parquet
    main_data = df.drop(columns=['product_data', 'customer_data', 'shipping'], errors='ignore')  # Main Data
    product_data = df[['product_name', 'product_slug', 'product_code', 'product_price', 'cogs']]  # Product Data
    customer_data = df[['customer_name', 'customer_email', 'customer_phone']]  # Customer Data
    shipping_data = df[['shipping_cost', 'shipping_mode']]  # Shipping Data

    # Save to Parquet
    main_data.to_parquet(f"{path_to_transform_dataset}/{dataset_transform_file}_main.parquet")
    product_data.to_parquet(f"{path_to_transform_dataset}/{dataset_transform_file}_product_data.parquet")
    customer_data.to_parquet(f"{path_to_transform_dataset}/{dataset_transform_file}_customer_data.parquet")
    shipping_data.to_parquet(f"{path_to_transform_dataset}/{dataset_transform_file}_shipping.parquet")

# Airflow DAG definition
with DAG(
    dag_id="etl_mongodb_to_gcs_to_bigquery",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    with TaskGroup('extract_transform_load') as etl_tasks:
        
        # Task 1: Extract and transform data into separate DataFrames for product_data, customer_data, and shipping
        transform_raw_data = PythonOperator(
            task_id='transform_raw_data',
            python_callable=extract_transform_data_task
        )
        
        # Task 2: Upload main data, product data, customer data, and shipping data to GCS
        upload_main_data_to_gcs = LocalFilesystemToGCSOperator(
            task_id='upload_main_data_to_gcs',
            src=f"{path_to_transform_dataset}/{dataset_transform_file}_main.parquet",
            dst=f"transform/{dataset_transform_file}_main.parquet",
            bucket=BUCKET
        )
        
        upload_product_data_to_gcs = LocalFilesystemToGCSOperator(
            task_id='upload_product_data_to_gcs',
            src=f"{path_to_transform_dataset}/{dataset_transform_file}_product_data.parquet",
            dst=f"transform/{dataset_transform_file}_product_data.parquet",
            bucket=BUCKET
        )
        
        upload_customer_data_to_gcs = LocalFilesystemToGCSOperator(
            task_id='upload_customer_data_to_gcs',
            src=f"{path_to_transform_dataset}/{dataset_transform_file}_customer_data.parquet",
            dst=f"transform/{dataset_transform_file}_customer_data.parquet",
            bucket=BUCKET
        )
        
        upload_shipping_data_to_gcs = LocalFilesystemToGCSOperator(
            task_id='upload_shipping_data_to_gcs',
            src=f"{path_to_transform_dataset}/{dataset_transform_file}_shipping.parquet",
            dst=f"transform/{dataset_transform_file}_shipping.parquet",
            bucket=BUCKET
        )

        # Task 3: Load the separate Parquet files to BigQuery
        load_main_data_to_bigquery = GCSToBigQueryOperator(
            task_id='load_main_data_to_bigquery',
            bucket=BUCKET,
            source_format="PARQUET",
            source_objects=f"transform/{dataset_transform_file}_main.parquet",
            destination_project_dataset_table=f"{PROJECT_ID}.demo_etl.transformed_main_data",
            write_disposition='WRITE_TRUNCATE'
        )

        load_product_data_to_bigquery = GCSToBigQueryOperator(
            task_id='load_product_data_to_bigquery',
            bucket=BUCKET,
            source_format="PARQUET",
            source_objects=f"transform/{dataset_transform_file}_product_data.parquet",
            destination_project_dataset_table=f"{PROJECT_ID}.demo_etl.transformed_product_data",
            write_disposition='WRITE_TRUNCATE'
        )

        load_customer_data_to_bigquery = GCSToBigQueryOperator(
            task_id='load_customer_data_to_bigquery',
            bucket=BUCKET,
            source_format="PARQUET",
            source_objects=f"transform/{dataset_transform_file}_customer_data.parquet",
            destination_project_dataset_table=f"{PROJECT_ID}.demo_etl.transformed_customer_data",
            write_disposition='WRITE_TRUNCATE'
        )

        load_shipping_data_to_bigquery = GCSToBigQueryOperator(
            task_id='load_shipping_data_to_bigquery',
            bucket=BUCKET,
            source_format="PARQUET",
            source_objects=f"transform/{dataset_transform_file}_shipping.parquet",
            destination_project_dataset_table=f"{PROJECT_ID}.demo_etl.transformed_shipping_data",
            write_disposition='WRITE_TRUNCATE'
        )

        # Task dependencies: Extract and transform -> Upload to GCS -> Load to BigQuery
        transform_raw_data >> [
            upload_main_data_to_gcs, 
            upload_product_data_to_gcs, 
            upload_customer_data_to_gcs, 
            upload_shipping_data_to_gcs
        ]
        
        upload_main_data_to_gcs >> load_main_data_to_bigquery
        upload_product_data_to_gcs >> load_product_data_to_bigquery
        upload_customer_data_to_gcs >> load_customer_data_to_bigquery
        upload_shipping_data_to_gcs >> load_shipping_data_to_bigquery
