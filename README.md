## UK crime data ##

UK Police Data Ingestion  

The aim of the project is to ingest data from uk police data API.  
To be more precise, this API about stop and searches by force:  
https://data.police.uk/docs/method/stops-force/  
  
We will investigate data from 'metropolitan' police force  

```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up
```
## Dags

![This is an image](/images/dags.png)  

```
make run-code
```
```
make run-tests
```
