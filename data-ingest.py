from pyspark.sql import SparkSession


config = {
    "file_path":"test.csv"
    "output":"kafka",
    "bootstrap_servers":"localhost:9092",
    "topic":"test",
    "file_format":"csv"
}

spark = SparkSession.builder.appName("data-ingest").getOrCreate()

def read_file():
    if config["file_format"].lower() == "csv":
        df = spark.read.option("header","true").csv(config["file_path"])
    elif config["file_format"].lower() == "json":
        df = spark.read.json(config["file_path"])
    else:
        print("Unsupported file format. please pass either csv/json")
        exit()
    return df

def write_df():
    target = config['output']
    if target.lower() == "kafka":
        df.selectExpr("CAST(value AS STRING)") \
            .write.format("kafka") \ 
            .option("kafka.bootstrap.servers", config["bootstrap_servers"]) \
            .option("topic", config["topic"]) \
            .save()
    elif target.lower() == 'postgres':
        df.write \
        .format("jdbc") \
        .option("url", config["postgres_url"]) \
        .option("dbtable", config["postgres_table"]) \
        .option("user", config["postgres_user"]) \
        .option("password", config["postgres_password"]) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    else:
        print(f"Invalid target {target}. pls choose postgres/kafka")



read_file()
write_df()
