import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import  *

if __name__ == "__main__":
    print("Hello")
    conf = get_spark_app_config()
    print("Lets learn SparkSession")
    spark = SparkSession \
        .builder \
        .appName("Test1") \
        .master("local[2]") \
        .getOrCreate()
    logger = Log4j(spark)
    initiaDF=spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[1])
    #initiaDF.show()
    selectedDF=initiaDF.select("Age","Gender","Country","State","family_history","treatment").where("Age>20")
    selectedDF.show()
