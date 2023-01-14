# Databricks notebook source
import csv
import logging
import json
from setLogger import set_logger
from taskLoader import readIntoTxt
import pandas as pd
log=set_logger()

dir_path=dbutils.widgets.get("dir_name")

final_file_name="final_covid_data.csv"
merge_csv=dir_path+'/' +final_file_name
file_task2= dir_path +'/' + "most_affected_country.txt"
file_task3= dir_path + '/' + "most_cases_country.txt"
file_task4=  dir_path + '/' + "most_recoveredcountry.txt"



writer=None
# Merge the result of multiple output file
def mergeTask(task1, task2,task3):
    new_file = open(merge_csv, 'w')
    log.info(f"writing tasks output in {merge_csv}")
    writer = csv.writer(new_file)
   
    if writer is None:
              raise Exception("exception occured")
    else:        
        writer.writerow(['Most_Affected_Country', 'Maximum_Covid_Cases', 'Most_Recovered_Cases'])
        writer.writerow([task1, task2, task3])
        log.info(f"{merge_csv} uploaded successfully")

        
   
    

            
                    



mostAffectedCountry = readIntoTxt(file_task2)
maxCaseCountry =readIntoTxt(file_task3)
mostRecoveredCountry= readIntoTxt(file_task4)
mergeTask(mostAffectedCountry,maxCaseCountry,mostRecoveredCountry)







# COMMAND ----------


