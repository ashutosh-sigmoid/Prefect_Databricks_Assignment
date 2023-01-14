# Databricks notebook source
import requests
import json
import logging
import pandas as pd
from setLogger import set_logger
from SparkSession import sparkSession



log=set_logger()



# get url_head,dir_path,rapid_api,country_list and iso_list

url_head=dbutils.widgets.get("url_name")
dir_path=dbutils.widgets.get("dir_name")
rapid_api_host=dbutils.widgets.get("rapid_api_host")
rapid_api_key=dbutils.widgets.get("rapid_api_key")
country_list=dbutils.widgets.get("countries_list")
iso_list=dbutils.widgets.get("iso_list")
headers={"X-RapidAPI-Key": rapid_api_key,"X-RapidAPI-Host":rapid_api_host}

key=country_list.split(',')
values=iso_list.split(',')

countries=[]
for i in  range(len(key)):
    d={}
    d['Country']=key[i]
    d['iso']=values[i]
    countries.append(d)




response_data=[]
def countryByname(country,iso_code):
    
 
    url=f"{url_head}/{country}/{iso_code}"
    
    log.info("Requesting Covid API...")
    try:
            response = requests.request("GET", url, headers=headers)
            if response.ok:
                log.info(f"API response status code: {response.status_code}")  
                response_data.append(response.json()[0])
                
                
                
    
    except Exception as e:
            log.info(f"Exception occurred while calling API: {e.__traceback__}")
            raise e




# COMMAND ----------


for country in countries:
        countryByname(country['Country'],country['iso'])
          




# COMMAND ----------





final_df = pd.DataFrame()
for i in range(20):
    country_data = response_data[i]
    temp_df = pd.DataFrame(data=[country_data])
    final_df = pd.concat([final_df, temp_df], ignore_index=True)


file_name ='covid_data.csv'

# initiate sparkSession

spark=sparkSession()

# create spark DataFrame
sparkDF=spark.createDataFrame(final_df)

# write spark dataframe to csv file
sparkDF.write.option("header",True).mode('overwrite').csv(dir_path+'/'+file_name)







        


