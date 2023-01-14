from pyspark.sql import SparkSession

# dir_path=dbutils.widgets.get("dir_name")
file_name="covid_data.csv"
def sparkSession():
    spark = SparkSession.builder.appName("SPARK").getOrCreate()
    df=spark.read.options(header='True', inferSchema='True').csv(f"/FileStore/tables/Ashutosh/{file_name}")
    return df 
  




