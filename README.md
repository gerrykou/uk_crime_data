## UK crime data ##

UK Police Data Ingestion  

```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up
```



```
make run-code
```
```
make run-tests
```
