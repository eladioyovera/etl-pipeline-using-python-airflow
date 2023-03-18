from io import StringIO
import os
from azure.storage.blob import ContainerClient
from google.cloud.storage import Client
import pandas as pd
from pandas import DataFrame
from pymongo import MongoClient
import sqlalchemy as db
from sqlalchemy import text


class Extract():
    def __init__(self) -> None:
        self.process = 'Extract Process'


    def read_cloud_storage(self, bucketName, fileName):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/eladio/airflow/dags/project_retail/keys/gcs_key.json"
        client = Client()
        bucket = client.get_bucket(bucketName)
        blob = bucket.get_blob(fileName)
        downloaded_file = blob.download_as_text(encoding="utf-8")
        df = pd.read_csv(StringIO(downloaded_file))
        return df


    def read_cloud_storage_without_headers(self, bucketName, fileName, headers):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/eladio/airflow/dags/project_retail/keys/gcs_key.json"
        client = Client()
        bucket = client.get_bucket(bucketName)
        blob = bucket.get_blob(fileName)
        downloaded_file = blob.download_as_text(encoding="utf-8")
        df = pd.read_csv(StringIO(downloaded_file), sep = '|', names = headers, header = None)
        return df


    def read_adls(self, containerName, fileName):
        conn_str = "BlobEndpoint=https://adlsdatapath.blob.core.windows.net/;QueueEndpoint=https://adlsdatapath.queue.core.windows.net/;FileEndpoint=https://adlsdatapath.file.core.windows.net/;TableEndpoint=https://adlsdatapath.table.core.windows.net/;SharedAccessSignature=sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-03-21T10:03:10Z&st=2023-02-22T02:03:10Z&spr=https,http&sig=thudqykpqiZUr2ty%2B%2B8HSkLvmQnb5ejHbMAr3YILHQ8%3D"
        container = containerName

        container_client = ContainerClient.from_connection_string(
            conn_str=conn_str, 
            container_name=container
        )

        downloaded_blob = container_client.download_blob(fileName)

        df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))

        return df
    

    def read_mongodb(self, databaseName, collectionName):        
            CONNECTION_STRING = "mongodb+srv://eyovera:Thenights04@cluster0.l1t5sr0.mongodb.net/?retryWrites=true&w=majority"     
            # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
            client = MongoClient(CONNECTION_STRING)

            dbname = client[databaseName]
 
            # Create a new collection
            collection_name = dbname[collectionName]
            collectionName = collection_name.find({})
            df = DataFrame(collectionName)

            return df


    def read_postgresql(self, databaseName, tableName):
        engine = db.create_engine(f'postgresql://eyovera:Postgresql@localhost:5432/{databaseName}')
        conn = engine.connect()

        sql_query = pd.read_sql_query(text(f'SELECT * FROM {tableName}'), conn)
        df = pd.DataFrame(sql_query)

        return df

