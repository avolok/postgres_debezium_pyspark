import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json
import pathlib
from pyspark.sql.functions import from_json, col, row_number, date_from_unix_date, expr
from pyspark.sql import Window
from delta import DeltaTable
from dataclasses import dataclass

@dataclass
class DeltaSettings():
    enable_deletion_vectors: bool = False
    enable_optimized_write: bool = False
    enable_auto_compact: bool = False
    tune_file_sizes_for_rewrite: bool = False
    
@dataclass
class StreamingSettings():
    processing_time: int = 15
    max_offsets_per_trigger: int = 50000
    
@dataclass
class Settings():
    streaming: StreamingSettings
    delta: DeltaSettings

class SimpleParser():
    
    def __init__(self):
        
        print("Initializing App")
        
        self.schema_path = "./schema"
        self.table_path = "/data/cdctable"        
        self.checkpoint_path = '../checkpoints/cdctable'
        
        self.settings: Settings = Settings(
            delta=DeltaSettings(            
                enable_optimized_write=self.__str2bool(os.environ.get("ENABLE_OPTIMIZED_WRITE", False)),
                enable_auto_compact=self.__str2bool(os.environ.get("ENABLE_AUTO_COMPACT", False)),
                tune_file_sizes_for_rewrite=self.__str2bool(os.environ.get("TUNE_FILE_SIZES_FOR_REWRITE", False)),
                enable_deletion_vectors=self.__str2bool(os.environ.get("ENABLE_DELETION_VECTORS", False)),
            ),
            streaming=StreamingSettings(
                processing_time=int(os.environ.get("STREAMING_PROCESSING_TIME", 15)),
                max_offsets_per_trigger=int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", 50000))
            )
        )
                
        self.spark = self.initialize_spark()
        self.target_delta: DeltaTable | None = self.initialize_table()
        
        
        
    def initialize_spark(self):
        spark =  (
            SparkSession
            .builder
            .master("local[*]")
            .appName('spark_demo')
            .config("spark.driver.extraJavaOptions", "-Dlog4jspark.root.logger=ERROR,console")
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0')
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.session.timeZone", "UTC")
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
            .config("spark.driver.extraJavaOptions", "-Ddelta.log.cacheSize=3")
            .config(
                "spark.driver.extraJavaOptions",
                "-XX:+CMSClassUnloadingEnabled -XX:+UseCompressedOops",
            )
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        print("Spark Initialized")        
        return spark
        
    def get_schema(self, schema_name="cdc_table"):        
        schema_path = pathlib.Path(f"{self.schema_path}/{schema_name}.json").read_text()
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
            .option("maxOffsetsPerTrigger", self.settings.streaming.max_offsets_per_trigger)
            .load()
            .select(
                from_json(col("value").cast("string"), schema).alias("parsed_value")              
            )
            .select("parsed_value.*")
        )

        return _df
    
    def _deduplicate(self, df):

        df_upsert = df.select("after.*", "source.txId", "op").where("op in ('c', 'u', 'r')")
        df_delete = df.select("before.*", "source.txId", "op").where("op in ('d')")

        df_combined = df_upsert.unionAll(df_delete)

        window_specs = Window.partitionBy('id').orderBy(col('txId').desc())

        df_deduplicated = df_combined.withColumn(
            "rn",
            row_number().over(window_specs)
        ).where("rn = 1").drop("rn", "txId")
        
        return df_deduplicated
    
    def _merge(self, df):

        delete_condition = expr("s.op = 'd'")
        
        (
            self.target_delta.alias("t")
            .merge(df.alias("s"), "s.id = t.id")
            .whenMatchedDelete(condition=delete_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll(condition=~delete_condition)
            .execute()
        )            
                

    def _transform(self, df):
        return df.withColumn("date", date_from_unix_date("date"))
    
    def foreach_batch_function(self, df, epoch_id):
        # Transform and write batchDF
        
        start = datetime.datetime.now()
        df.cache()
                
        # 3 stages to process: remove duplicates, apply type transformations and then upsert to the destination table
        _d = self._deduplicate(df)  
        _t = self._transform(_d)   
        _t.cache()   
        self._merge(_t)
        
        finish = datetime.datetime.now()
        time_spent = finish - start
        
        print("Merging dataframe:")
        _t.show(truncate=False)
        
        print(f"Rows processed: {_t.count()}, time spent: {time_spent}ms")
        
        _t.unpersist()
        df.unpersist()
    
    def start_streaming(self):
        
        print("The number of messages in the kafka queue: ", self.read_data().count())
        
        print(f"Starting Streaming with a trigger: processingTime={self.settings.streaming.processing_time} seconds")
        print(f"Max offsets per trigger: {self.settings.streaming.max_offsets_per_trigger}")
        

        readStream = self.read_streaming_data()
        
        command = (
            readStream
            .writeStream
            .option("checkpointLocation", self.checkpoint_path)
            .foreachBatch(self.foreach_batch_function)
            .trigger( processingTime=f"{self.settings.streaming.processing_time} seconds")
            .start()            
        )        
        
        command.awaitTermination()  
        
    def get_table_content(self):
        return DeltaTable.forPath(self.spark, self.table_path).toDF().show()
    
    @staticmethod
    def __str2bool(v) -> bool:
        return v.lower() in ("yes", "true", "t", "1")
    
    def initialize_table(self) -> DeltaTable:
        table_exists = DeltaTable.isDeltaTable(self.spark, self.table_path)
        
        if table_exists:
            print("Table already exists")
        else:
            print("Creating an empty table")
            self.spark.createDataFrame([], self.get_schema("target_table")).write.format("delta").save(self.table_path)
        
        if self.settings.delta.enable_deletion_vectors:
            self.spark.sql(f"""
                ALTER TABLE delta.`{self.table_path}` 
                SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
            """)
            print ("Enabling deletion vectors")
            
        
        if self.settings.delta.enable_optimized_write:
            self.spark.sql(f"""
                ALTER TABLE delta.`{self.table_path}` 
                SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true);
            """)
            
            print ("Enabling optimized write")
            
        if self.settings.delta.enable_auto_compact:
            self.spark.sql(f"""
                ALTER TABLE delta.`{self.table_path}` 
                SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = true);
            """)
            
            print ("Enabling auto compaction")
            
        if self.settings.delta.tune_file_sizes_for_rewrite:
            self.spark.sql(f"""
                ALTER TABLE delta.`{self.table_path}` 
                SET TBLPROPERTIES ('delta.tuneFileSizesForRewrites' = true);
            """)
            
            print ("Enabling 'tune file sizes for rewrites'")
            
        return DeltaTable.forPath(self.spark, self.table_path)            