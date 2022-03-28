## UK crime data ##

UK Police Data Ingestion  

The aim of the project is to ingest data from uk police data API to Gcloud - BigQuery.  
To be more precise, data is coming from this API about stop and searches by force:  
https://data.police.uk/docs/method/stops-force/  
  
We will investigate data from 'metropolitan' police force.  

Create your google credentials and save the file in this path   
~/.google/credentials/   

Update the docker-compose.yaml file with your project id and google storage bucket   
```
GCP_PROJECT_ID: 'de-bootcamp-339509'
GCP_GCS_BUCKET: 'dtc_data_lake_de-bootcamp-339509'
```

```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up
```
## Dags

![data_ingestion_dag](/images/dag_data_ingestion.png)  
![gcs_2_bq_dag](/images/dag_gcs_2_bq.png)  
## Terraform
```
gcloud auth application-default login 
```
```
terraform init
terraform plan
terraform apply
```
## Queries  

![Queries](/images/query-object-of-search.png) 
<!-- ```
make run-code
```
```
make run-tests
``` -->
