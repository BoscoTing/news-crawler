from pyspark.sql import SparkSession
from typing import Optional, Dict


def create_spark_session(
    app_name: str,
    s3_access_key: Optional[str] = None,
    s3_secret_key: Optional[str] = None,
    s3_endpoint: Optional[str] = None,
    additional_options: Optional[Dict] = None,
) -> SparkSession:
    """Create a SparkSession with S3 configurations.
    
    Args:
        app_name: Name of the Spark application
        s3_access_key: AWS access key ID
        s3_secret_key: AWS secret access key 
        s3_endpoint: Custom S3 endpoint (for MinIO etc.)
        additional_options: Additional Spark configurations
        
    Returns:
        Configured SparkSession
    """
    # Define AWS dependencies
    hadoop_aws_version = "3.3.4"
    aws_sdk_version = "1.12.261"
    
    packages = [
        f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version}",
        f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}",
    ]
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", ",".join(packages)) \

    # S3A filesystem configuration
    builder = builder \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    if s3_access_key and s3_secret_key:
        builder = builder \
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
    
    # Custom endpoint for S3-compatible storage
    if s3_endpoint:
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
    
    # Performance tuning for S3
    builder = builder \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "300000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "20") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.multipart.size", "64M") \
        .config("spark.hadoop.fs.s3a.multipart.threshold", "64M") \
        .config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536") \
        .config("spark.hadoop.fs.s3a.socket.send.buffer", "65536") \
        .config("spark.hadoop.fs.s3a.threads.max", "20") \
        .config("spark.hadoop.fs.s3a.max.total.tasks", "100") \
        .config("spark.hadoop.fs.s3a.readahead.range", "1M")
    
    if additional_options:
        for key, value in additional_options.items():
            builder.config(key, value)
    
    return builder.getOrCreate()
