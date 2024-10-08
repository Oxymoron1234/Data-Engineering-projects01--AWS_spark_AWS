import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *
jar_path_windows = "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar"
jar_path_mac = "/Users/ashishkumarjha/spark-3.5.3-bin-hadoop3/jars/mysql-connector-j-9.0.0.jar"
def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("sales_spark2")\
        .config("spark.driver.extraClassPath",jar_path_mac)\
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark

