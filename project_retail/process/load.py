import io
from io import StringIO
import os
from azure.storage.blob import ContainerClient
from google.cloud import bigquery
from google.cloud.storage import Client
import sqlalchemy as db
from sqlalchemy import Column, create_engine, MetaData, Table 
from sqlalchemy import text
import pandas as pd


class Load():
    def __init__(self) -> None:
        self.process = 'Load Process'


    def load_to_adls(self, df, containerName, blobName):
        pass


    def load_to_cloud_storage(self, df, bucketName, fileName):       
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/eladio/airflow/dags/project_retail/keys/gcs_key.json"
        client = Client()
        bucket = client.get_bucket(bucketName)
        bucket.blob(fileName).upload_from_string(df.to_csv(index=False), 'text/csv',)
    
    
    def load_to_bigquery(self, df, Dataset, tableName):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/eladio/airflow/dags/project_retail/keys/bigquery_key.json"
        client = bigquery.Client()

        # Para configurar si se agrega o se va truncando
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE)

        # Parametrizamos el dataset y nombre de la tabla
        table = f'dynamic-reef-374823.{Dataset}.{tableName}'

        # Load data to BQ
        job = client.load_table_from_dataframe(df, table, job_config=job_config)


    def postgresql_engine(self, user = 'eyovera', password = 'Postgresql', host = 'localhost', port = '5432', database = 'retail'):
        engine = create_engine("postgresql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))
        return engine
    

    def df_to_postgresql(self, df, engine, tbl_name):
        df.to_sql(tbl_name, engine, if_exists='replace')


    def load_to_postgresql(self, df, db_tbl_name):
        self.df_to_postgresql(df, self.postgresql_engine(), db_tbl_name)