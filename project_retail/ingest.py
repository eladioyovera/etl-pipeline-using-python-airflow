from process.extract import Extract
from process.load import Load


extract = Extract()
load = Load()


# EXTRACT SOURCES

# GCS
df_order_items = extract.read_cloud_storage("retail-ey","order_items")
df_orders = extract.read_cloud_storage("retail-ey","orders")

# Azure Data Lake
df_customer = extract.read_adls("retail", "customer")

# MongoDB
df_departments = extract.read_mongodb("retail","departments")

# Postgresql
df_products = extract.read_postgresql("retail","products")
df_categories = extract.read_postgresql("retail","categories")


# LOAD DATA TO LANDING LAYER - DATA LAKE (GCS)

load.load_to_cloud_storage(df_order_items,"data-lake-retail", "landing/order_items")
load.load_to_cloud_storage(df_orders,"data-lake-retail", "landing/orders")
load.load_to_cloud_storage(df_customer,"data-lake-retail", "landing/customer")
load.load_to_cloud_storage(df_departments,"data-lake-retail", "landing/departments")
load.load_to_cloud_storage(df_products,"data-lake-retail", "landing/products")
load.load_to_cloud_storage(df_categories,"data-lake-retail", "landing/categories")
