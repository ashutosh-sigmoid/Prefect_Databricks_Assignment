# Databricks notebook source
import requests
import json
import logging
import pandas as pd

from setLogger import set_logger
from SparkSession import sparkSession

log=set_logger()




url = "https://vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com/api/npm-covid-data/countries"

headers = {
	"X-RapidAPI-Key": "c7733420admsh926a4b09e2d344fp1a2e7cjsnc476b536b77b",
	"X-RapidAPI-Host": "vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com"
}

response_data=[]
log.info("Requesting Covid API...")
try:
    response = requests.request("GET", url, headers=headers)
    if response.ok:
            log.info(f"API response status code: {response.status_code}")
            response_data = response.json()


except Exception as e:
        log.info(f"Exception occurred while calling API: {e.__traceback__}")
        raise e




# COMMAND ----------

final_df = pd.DataFrame()
for i in range(20):
    country_data = response_data[i]


    temp_df = pd.DataFrame(data=[country_data])
    final_df = pd.concat([final_df, temp_df], ignore_index=True)


# final_df.to_csv("/covid_data.csv",index=False)
file_name = 'covid_data.csv'
dir_path = '/dbfs/FileStore/tables/Ashutosh'
final_df.to_csv(dir_path+'/'+file_name,index=False,encoding="utf-8")

log.info("Spark session initiated")
df=sparkSession()
log.info("Dataframe created")
df.show()



# df=spark.read.csv('/FileStore/tables/Ashutosh/covid_data.csv',inferSchema=True,header=True)
# df.show(n=20,truncate=False)
