# Databricks notebook source
import requests
import json
import logging
import pandas as pd

from setLogger import set_logger
from SparkSession import sparkSession

log=set_logger()





url=dbutils.widgets.get("url_name")
dir_path=dbutils.widgets.get("dir_name")
rapid_api_host=dbutils.widgets.get("rapid_api_host")
rapid_api_key=dbutils.widgets.get("rapid_api_key")
headers={"X-RapidAPI-Key": rapid_api_key,"X-RapidAPI-Host":rapid_api_host}



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



file_name ='covid_data.csv'

final_df.to_csv(dir_path+'/'+file_name,index=False,encoding="utf-8")

log.info("Spark session initiated")
df=sparkSession()
log.info("Dataframe created")
df.show()




