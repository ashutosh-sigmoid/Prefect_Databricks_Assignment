# Databricks notebook source
from pyspark.sql.functions import col, lit, first, round, last
from pyspark.sql import SparkSession
from SparkSession import readCsv
from setLogger import set_logger
from taskLoader import *
log=set_logger()


dir_name=dbutils.widgets.get("dir_name")

# maximum case of covid
def maxCaseCountry(covidData):
    log.info("Maxcase Country ")
    countriesWithHighestCovid = covidData.select(col("Country"), col("TotalCases")).orderBy(col("TotalCases").desc()).first()['Country']
    return countriesWithHighestCovid


covidData=readCsv(spark)
countriesWithHighestCovid = maxCaseCountry(covidData)
log.info("countriesWithHighestCovid")
loadIntoTxt(countriesWithHighestCovid,dir_name,"most_cases_country.txt")

