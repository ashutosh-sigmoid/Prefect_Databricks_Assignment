from pyspark.sql import SparkSession

dir_path="/FileStore/tables/Ashutosh"
file_name="covid_data.csv"
def sparkSession():
    spark = SparkSession.builder.appName("SPARK").getOrCreate()
    df=spark.read.options(header='True', inferSchema='True').csv("/FileStore/tables/Ashutosh/covid_data.csv")
    return df 
  




