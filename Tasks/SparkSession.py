from pyspark.sql import SparkSession


file_name="covid_data.csv"
def sparkSession():
        spark = SparkSession.builder.appName("SPARK").getOrCreate()
        return spark
    
def readCsv(spark):    
    df=spark.read.options(header='True', inferSchema='True').csv(f"/FileStore/tables/Ashutosh/{file_name}")
    return df 
  




