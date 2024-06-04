# Postgres to Spark using Structured Streaming, Kafka and Debezium

The intention of this repo is a compact lab that runs end-to-end replication from Postgres to Delta Lake using Kafka, Debezim and Spark Structured Streaming

The project has a topology:

```text
pg-data-producer -> postgres -> debezium -> kafka -> spark-data-consumer (spark streaming)
                                                  -> py-app-consumer (for console output)
```
In a nutshell, following steps performed:

- Python app that sends merge statements to Postgres database
- Debezium connect watches for changes in the database and sends it to Kafka
- Spark reads data from Kafka using structured streaming and writing data using delta
- Python app consumer reads kafka topic and sends the output to the console

## Requirements
- Docker Engine
- Make
- MacOS/Linux/WSL

## How to start

Build custom images:

```bash
make build_all 
```

Spin-up the environment

```bash
make run
```

When the tests are done, you can destroy the environment:

```bash
make destroy
```