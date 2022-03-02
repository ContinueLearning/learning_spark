import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config()
    print("Lets learn SparkSession")
    spark = SparkSession \
                .builder \
                .appName("HelloSpark") \
                .master("local[2]") \
                .getOrCreate()

    logger = Log4j(spark)
    survey_raw_df = load_survey_df(spark, sys.argv[1])
    survey_raw_df.show()
    logger.warn("Welcome to Spark Session")
    #Below is a comment..
    #survey_raw_df.select("Age").show(10)
    #survey_raw_df.filter("Age>40").show(10)
    survey_raw_df_filtered=survey_raw_df.where("Age>40") \
                            .select("Age","Gender","Country","state")
    survey_raw_df_filtered.show(20)
    logger.warn("After selection, before grouping")
    survey_raw_df_filtered.groupBy("Country").show()

