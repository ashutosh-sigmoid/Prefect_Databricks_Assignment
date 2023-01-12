# Databricks notebook source
from pyspark.sql.functions import col, lit, first, round, last
from pyspark.sql import SparkSession
from SparkSession import sparkSession
from setLogger import set_logger
from taskLoader import *

log=set_logger()
spark_df=sparkSession()


def mostRecoveredCountry(covidData):
    log.info("Recovered country")
    res = covidData.withColumn("Recovered_Case/Total_Case", lit(round(col("TotalRecovered") / col("TotalCases"), 3)))
    res = res.select(col("Country"), col("Recovered_Case/Total_Case")).orderBy(
        col("Recovered_Case/Total_Case").desc()).first()["Country"]
    return res




covidData=sparkSession()
mostrecoveredcountry = mostRecoveredCountry(covidData)
log.info(f"mostRecoveredCountry")
loadIntoTxt(mostrecoveredcountry,"most_recoveredcountry.txt")
readIntoTxt("most_recoveredcountry.txt")


