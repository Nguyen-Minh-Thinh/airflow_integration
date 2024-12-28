from pyspark.sql import SparkSession
import os
import datetime
import pathlib 
from dotenv import dotenv_values 
"""
spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --deploy-mode client --executor-memory 512m --executor-cores 1 --num-executors 1 spark.py

spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1 spark.py
"""

# Get the directory path absolutely that contains this file
script_path = pathlib.Path(__file__).parent.resolve()

config = dotenv_values(f"{script_path}/.env")


# Thông tin kết nối MinIO
minio_endpoint = config["MINIO_ENDPOINT"]  # Thay đổi thành endpoint MinIO của bạn
minio_access_key = config["MINIO_ACCESS_KEY"]  # Access key
minio_secret_key = config["MINIO_SECRET_KEY"]  # Secret key
bucket_name = config["MINIO_BUCKET_NAME"]  # Tên bucket
output_path = f"s3a://{bucket_name}/kafka-output"
dt = datetime.datetime.now()

try:
    # Khởi tạo SparkSession
    spark = (
        SparkSession
        .builder
        .appName("Kafka Test2")
        # Dam bao khi stop ung dung streaming thi cac du lieu dang tren line van duoc xu ly roi moi shutdown
        .config('spark.streaming.stopGracefullyOnShutdown', True) 
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", True)  
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # Thiết lập mức log để giảm chi tiết không cần thiết
    print("SparkSession initialized successfully.")
except Exception as e:
    print(f"Error initializing SparkSession: {e}")
    raise

try:
    # Đọc dữ liệu từ Kafka
    print("Connecting to Kafka topic: test_airflow_integration")
    kafka_df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test_airflow_integration")  # Tên topic Kafka
        .option("startingOffsets", "earliest")
        .load()
    )
    print("Successfully connected to Kafka and started reading stream.")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    raise

try:
    # Chuyển đổi dữ liệu từ binary sang string
    kafka_df_cast = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    kafka_df_cast.printSchema()
    print("Schema printed successfully.")
except Exception as e:
    print(f"Error processing Kafka stream: {e}")
    raise

checkpoint_location = "checkpoints/kafka_streaming"
os.makedirs(checkpoint_location, exist_ok=True)  # Tạo thư mục nếu chưa tồn tại
print(f"Checkpoint location set to: {checkpoint_location}")



def data_output(batch_df, batch_id):
    print("Batch ID:", batch_id)
    batch_df.show()

    # Ghi từng batch vào MinIO
    try:
        batch_df.write.format("json").mode("append").save(f'{output_path}/{dt.year}/{dt.month}/{dt.day}')
        print(f"Batch {batch_id} ghi thành công vào MinIO.")
    except Exception as e:
        print(f"Lỗi khi ghi batch {batch_id}: {e}")
        raise

try:
    query = (
        kafka_df_cast.writeStream
        .foreachBatch(data_output)  # Sử dụng từng batch
        .trigger(processingTime="20 seconds")
        .option("checkpointLocation", checkpoint_location)  
        .start()
    )
    print("Streaming query started. Writing to MinIO in batch mode...")
    
    query.awaitTermination()
except Exception as e:
    print(f"Error starting streaming query: {e}")
    raise