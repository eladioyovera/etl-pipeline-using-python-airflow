# etl-pipeline-using-python-airflow

This is an end-to-end sample project on building a data pipeline from different sources, such as MongoDB, Azure Blob Storage, Google Cloud Storage, and PostgreSQL Database. For the storage layer we use Google Cloud services, for our data lake we use GCS and for the data warehouse we use Bigquery which can be used for analysis and reporting purposes. For the processing layer we use pandas, in which we apply all the business rules that the user provides us.
Also, since it is an end-to-end project, the insights can be observed in DataStudio.
Finally for the orchestration layer we use Apache Airflow.

The Retail.SA company wishes to build a data repository that allows it to learn more about its customers and make decisions to improve its service and operation.

### 1. DATA ARCHITECTURE

![image](https://user-images.githubusercontent.com/96121226/226145281-3f140771-9d4c-4c71-8894-ad5c8eac0630.png)

#### A. INGESTION LAYER
At this point, the connection settings must be made to extract the data from the different sources, as well as the settings for our data load. In this case, a class is created for both extract and load, and functions are created for each source and destination using python. The permissions that are assigned to read and write data in google cloud, Azure are not detailed in this writing, however, in a future version of the project the links will be added where the details of the permission assignment will be displayed.
