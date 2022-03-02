
from pyspark.sql import SparkSession
from lib.logger import Log4j
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
if __name__=="__main__":
    spark= SparkSession. \
           builder. \
           master("local[3]"). \
           appName("SparkDemo"). \
           getOrCreate()
    logger=Log4j(spark)
    print("Done initiating Logger")

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])
    flightTimeCsvDF = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(flightSchemaStruct) \
    .option("mode", "FAILFAST") \
    .option("dateFormat", "M/d/y") \
    .load("data/flight*.csv")
    flightTimeCsvDF.show(5)
    print("***********")

    flightTimeCsvDF.select("FL_Date").where("FL_Date>'2000-01-01'")