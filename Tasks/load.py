# Databricks notebook source
import csv
import logging
import json
from setLogger import set_logger
from taskLoader import readIntoTxt
from SparkSession import *
log=set_logger()
import pandas as pd





dir_path="/dbfs/FileStore/tables/Ashutosh"
file_name="final_covid_data.csv"
merge_csv=dir_path+ '/'+file_name
file_task2= dir_path +'/'+"most_affected_country.txt"
file_task3= dir_path +'/'+"most_cases_country.txt"
file_task4= dir_path +'/'+"most_recoveredcountry.txt"



# Merge the result of multiple output file
def mergeTask(task1, task2,task3):
    new_file = open(merger_csv, 'w')
    log.info(f"writing tasks output in {merge_csv}")
    writer = csv.writer(new_file)
    writer.writerow(['Most_Affected_Country', 'Maximum_Covid_Cases', 'Most_Recovered_Cases'])
    writer.writerow([task1, task2, task3])
    log.info(f" {merger_csv} uploaded successfully")
    
    
    

# COMMAND ----------

mostAffectedCountry = readIntoTxt(file_task2)
maxCaseCountry =readIntoTxt(file_task3)
mostRecoveredCountry= readIntoTxt(file_task4)
mergeTask(mostAffectedCountry,maxCaseCountry,mostRecoveredCountry)




# COMMAND ----------


