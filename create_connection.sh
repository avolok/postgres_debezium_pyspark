apt-get -y update; apt-get -y install curl;
curl --location 'http://connect:8083/connectors' \
   --header 'Accept: application/json' \
   --header 'Content-Type: application/json' \
   --data '{
   "name": "cdc-using-debezium-connector",
   "config": {
       "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
       "database.hostname": "db",
       "database.port": "5432",
       "database.user": "postgres",
       "database.password": "postgres",
       "database.dbname": "volok1",
       "database.server.id": "184054",
       "table.include.list": "public.cdctable",
       "topic.prefix": "debezium",
       "plugin.name": "pgoutput",
       "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
		"value.converter.schemas.enable": "false"
   }
}'