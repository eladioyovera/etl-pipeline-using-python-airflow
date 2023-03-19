# etl-pipeline-using-python-airflow

This is an end-to-end sample project on building a data pipeline from different sources, such as MongoDB, Azure Blob Storage, Google Cloud Storage, and PostgreSQL Database. For the storage layer we use Google Cloud services, for our data lake we use GCS and for the data warehouse we use Bigquery which can be used for analysis and reporting purposes. For the processing layer we use pandas, in which we apply all the business rules that the user provides us.
Also, since it is an end-to-end project, the insights can be observed in DataStudio.
Finally for the orchestration layer we use Apache Airflow.

The Retail.SA company wishes to build a data repository that allows it to learn more about its customers and make decisions to improve its service and operation.

### 1. DATA ARCHITECTURE

![image](https://user-images.githubusercontent.com/96121226/226145281-3f140771-9d4c-4c71-8894-ad5c8eac0630.png)

#### A. INGESTION LAYER
At this point, the connection settings must be made to extract the data from the different sources, as well as the settings for our data load. In this case, a class is created for both extract and load, and functions are created for each source and destination using python. The permissions that are assigned to read and write data in google cloud, Azure are not detailed in this writing, however, in a future version of the project the links will be added where the details of the permission assignment will be displayed.

#### B. STORAGE LAYER
At this point, since we perform the upload process locally, and also because we are using Google Cloud, it is necessary to generate the service account, which will allow us to load the data correctly. In the data lake part, we create a bucket in GCS with the name "data-lake-retail" in which the raw data is located. On the other hand, in Bigquery we create a dataset with the name “dwh-retail”, in which the processed data is ready for analysis and reporting.

#### C. PROCESSING LAYER
At this point we apply all the business rules that the client grants us. The module to carry out the development is pandas.

#### D. VISUALIZATION LAYER
We design the dashboard taking into account the requirements that the client wishes to observe, for this we use DataStudio which is connected to the Bigquery tables, where the data is ready to be consumed.
We attach the link to see the simple dashboard.

![image](https://user-images.githubusercontent.com/96121226/226147977-880ac5d4-e3f5-49c6-8410-9fcec9797122.png)


https://lookerstudio.google.com/reporting/89aea318-7f80-46d7-b2b5-6b96fb9b9023/page/aqZID


#### E. ORCHESTRATION LAYER
For this point, we develop a script that performs data ingest and transfor using apache airflow.

![image](https://user-images.githubusercontent.com/96121226/226147989-6df31f2a-3386-49d3-98ac-2655a2415b84.png)

