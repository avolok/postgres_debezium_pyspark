## How to start

```
docker compose up  --attach pg_data_producer --attach tiny_app_consumer --attach pyspark
```

## Topology

### Real world:

db -> debezium connect -> Even Hub -> Databricks


### Lab
pg-data-producer -> db -> debezium connect -> kafka -> spark-data-consumer (spark streaming app)
                                                    -> py-app-consumer (console app)
