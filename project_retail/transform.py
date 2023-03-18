from process.extract import Extract
from process.load import Load
from process.transform import Transform
import sqlalchemy as db
import pandas as pd


extract = Extract()
transform = Transform()
load = Load()


df_customer = extract.read_cloud_storage_without_headers("data-lake-retail", "landing/customer", ["customer_id","customer_fname","customer_lname","customer_email","customer_password","customer_street","customer_city","customer_state","customer_zipcode"])
df_orders = extract.read_cloud_storage_without_headers("data-lake-retail", "landing/orders", ["order_id", "order_date", "order_customer_id", "order_status"])
df_order_items = extract.read_cloud_storage_without_headers("data-lake-retail", "landing/order_items", ["order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal", "order_item_product_price"])
df_departments = extract.read_cloud_storage("data-lake-retail", "landing/departments")
df_products = extract.read_cloud_storage("data-lake-retail", "landing/products")
df_categories = extract.read_cloud_storage("data-lake-retail", "landing/categories")


df_question1 = transform.enunciado1(df_customer, df_orders, df_order_items)
df_question2 = transform.enunciado2(df_order_items, df_products, df_categories)
df_question3 = transform.enunciado3(df_customer, df_orders, df_order_items, df_products, df_categories)
df_question4 = transform.enunciado4(df_customer, df_orders, df_order_items, df_products, df_categories)
df_question5 = transform.enunciado5(df_orders, df_order_items, df_products, df_categories, df_departments)


#### Load to BigQuery
load.load_to_bigquery(df_question1, "dwh_retail", "df_question1")
load.load_to_bigquery(df_question2, "dwh_retail", "df_question2")
load.load_to_bigquery(df_question3, "dwh_retail", "df_question3")
load.load_to_bigquery(df_question4, "dwh_retail", "df_question4")
load.load_to_bigquery(df_question5, "dwh_retail", "df_question5")


db = 'retail'

question1 = "question1"
question2 = "question2"
question3 = "question3"
question4 = "question4"
question5 = "question5"


#### Load to postgresql
load.load_to_postgresql(df_question1, question1)
load.load_to_postgresql(df_question2, question2)
load.load_to_postgresql(df_question3, question3)
load.load_to_postgresql(df_question4, question4)
load.load_to_postgresql(df_question5, question5)