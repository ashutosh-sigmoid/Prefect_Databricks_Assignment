# Databricks notebook source
from pyspark.sql.functions import col, lit, first, round, last
from pyspark.sql import SparkSession
from SparkSession import readCsv
from setLogger import set_logger
from taskLoader import *

log=set_logger()

dir_name=dbutils.widgets.get("dir_name")

def mostRecoveredCountry(covidData):
    log.info("Recovered country")
    res = covidData.withColumn("Recovered_Case/Total_Case", lit(round(col("TotalRecovered") / col("TotalCases"), 3)))
    res = res.select(col("Country"), col("Recovered_Case/Total_Case")).orderBy(
        col("Recovered_Case/Total_Case").desc()).first()["Country"]
    return res




covidData=readCsv(spark)
mostrecoveredcountry = mostRecoveredCountry(covidData)
log.info(f"mostRecoveredCountry")
loadIntoTxt(mostrecoveredcountry,dir_name,"most_recoveredcountry.txt")



