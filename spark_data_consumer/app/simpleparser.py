import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json
import pathlib
from pyspark.sql.functions import from_json, col, row_number, date_from_unix_date, expr
from pyspark.sql import Window
from delta import DeltaTable

class SimpleParser():
    
    def __init__(self):
        
        print("Initializing App")
        
        self.spark =  (
            SparkSession
            .builder
            .master("local[*]")
            .appName('spark_demo')
            .config("spark.driver.extraJavaOptions", "-Dlog4jspark.root.logger=WARN,console")
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0')
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.dagGraph.retainedRootRDDs", "1")
            .config("spark.ui.retainedJobs", "1")
            .config("spark.ui.retainedStages", "1")
            .config("spark.ui.retainedTasks", "1")
            .config("spark.sql.ui.retainedExecutions", "1")
            .config("spark.worker.ui.retainedExecutors", "1")
            .config("spark.worker.ui.retainedDrivers", "1")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.driver.extraJavaOptions", "-Ddelta.log.cacheSize=3")
            .config(
                "spark.driver.extraJavaOptions",
                "-XX:+CMSClassUnloadingEnabled -XX:+UseCompressedOops",
            )
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")
        
        self.schema_path = "./schema/cdctable.json"
        self.table_path = "../data/cdctable"        
        self.checkpoint_path = '../checkpoints/cdctable'
        self.streaming_processing_tine: int = int(os.environ.get("STREAMING_PROCESSING_TIME", 15))
        
        print("Spark Initialized")
        
    def get_schema(self):
        schema_path = pathlib.Path(self.schema_path).read_text()
        schema = StructType.fromJson(json.loads(schema_path))        
        return schema
    
    def read_data(self): 
        schema = self.get_schema()      
        
        _df = (
            self.spark
            .read
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka1:9092")
            .option("subscribe", "debezium.public.cdctable")
            .load()
            .select(
                from_json(col("value").cast("string"), schema).alias("parsed_value")              
            )
            .select("parsed_value.*")
        )

        return _df
    
    def read_streaming_data(self): 
        
        schema = self.get_schema()      
        
        _df = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka1:9092")
            .option("subscribe", "debezium.public.cdctable")
            .option("startingOffsets", "earliest")
            .option("auto.offset.reset", "earliest")
            .load()
            .select(
                from_json(col("value").cast("string"), schema).alias("parsed_value")              
            )
            .select("parsed_value.*")
        )

        return _df
    
    def _processing_deduplicate(self, df):

        df_upsert = df.select("after.*", "source.txId", "op").where("op in ('c', 'u', 'r')")
        df_delete = df.select("before.*", "source.txId", "op").where("op in ('d')")

        df_combined = df_upsert.unionAll(df_delete)

        window_specs = Window.partitionBy('id').orderBy(col('txId').desc())

        df_deduplicated = df_combined.withColumn(
            "rn",
            row_number().over(window_specs)
        ).where("rn = 1").drop("rn", "txId")
        
        return df_deduplicated
    
    def _processing_upsert(self, df):
        table_exists = DeltaTable.isDeltaTable(self.spark, self.table_path)

        if table_exists:
            dt = DeltaTable.forPath(self.spark, self.table_path)            
            
            delete_condition = expr("s.op = 'd'")
            
            (
                dt.alias("t")
                .merge(df.alias("s"), "s.id = t.id")
                .whenMatchedDelete(condition=delete_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll(condition=~delete_condition)
                # fixme: add deletion case
                .execute()
            )            
        else:
            df.repartition(1).write.format("delta").save(self.table_path)
                

    def _processing_transform(self, df):
        return df.withColumn("date", date_from_unix_date("date"))
    
    def foreach_batch_function(self, df, epoch_id):
        # Transform and write batchDF
        
        start = datetime.datetime.now()
        df.cache()
                
        # 3 stages to process: remove duplicates, apply type transformations and then upsert to the destination table
        _d = self._processing_deduplicate(df)  
        _t = self._processing_transform(_d)      
        self._processing_upsert(_t)
        
        finish = datetime.datetime.now()
        time_spent = finish - start
        
        # print(f"Events processed: {df.count()}, time spent: {time_spent}ms")
        
        _t.show(truncate=False)
        
        df.unpersist()
    
    def start_streaming(self):
        
        print(f"Starting Streaming with a trigger: processingTime={self.streaming_processing_tine} seconds")
        
        readStream = self.read_streaming_data()
        
        command = (
            readStream
            .writeStream
            .option("checkpointLocation", self.checkpoint_path)
            .foreachBatch(self.foreach_batch_function)
            .trigger(processingTime=f"{self.streaming_processing_tine} seconds")
            .start()            
        )        
        
        command.awaitTermination()  
        
    def get_table_content(self):
        return DeltaTable.forPath(self.spark, self.table_path).toDF().show()