# Databricks notebook source
from pyspark.sql.functions import col, lit, round
from SparkSession import sparkSession
from setLogger import set_logger
from taskLoader import *

log=set_logger()






# mostAffectedCountry
def mostAffectedCountry(covidData):
    log.info("reading Data From Spark  Dataframe ")
    res = covidData.withColumn("Total_Deaths/Total_Cases",
                                   lit(round(col("TotalDeaths") / col("TotalCases"), 3)))

    res = res.select(col("Country"), col("Total_Deaths/Total_Cases")).orderBy(
        col("Total_Deaths/Total_Cases").desc()).first()['Country']

    return res





covidData=sparkSession()
mostAffectedCountry = mostAffectedCountry(covidData)
log.info("MostAffectedCountry")
loadIntoTxt(mostAffectedCountry,"most_affected_country.txt")




