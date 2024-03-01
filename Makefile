build_producer:
	cd tiny_app_producer;docker build -t python-tiny-app-producer .

build_consumer:
	cd tiny_app_consumer;docker build -t python-tiny-app-consumer .

build_pg_data_producer:
	cd pg_data_producer;docker build -t python-pg-data-producer .

build_spark_data_consumer:
	cd spark_data_consumer;docker build -t spark-data-consumer .

build_all: build_pg_data_producer build_spark_data_consumer
