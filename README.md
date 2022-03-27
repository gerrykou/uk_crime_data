## UK crime data ##

UK Police Data Ingestion  

The aim of the project is to ingest data from uk police data API to Gcloud - BigQuery.  
To be more precise, data is coming from this API about stop and searches by force:  
https://data.police.uk/docs/method/stops-force/  
  
We will investigate data from 'metropolitan' police force.  

```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up
```
## Dags

![Dags](/images/dags.png)  

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
