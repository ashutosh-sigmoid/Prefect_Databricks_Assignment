# Databricks notebook source
from pyspark.sql.functions import col, lit, first, round, last
from pyspark.sql import SparkSession
from SparkSession import sparkSession
from setLogger import set_logger
from taskLoader import *
log=set_logger()




# maximum case of covid
def maxCaseCountry(covidData):
    log.info("Maxcase Country ")
    countriesWithHighestCovid = covidData.select(col("Country"), col("TotalCases")).orderBy(col("TotalCases").desc()).first()['Country']
    return countriesWithHighestCovid


covidData=sparkSession()
countriesWithHighestCovid = maxCaseCountry(covidData)
log.info("countriesWithHighestCovid")
loadIntoTxt(countriesWithHighestCovid,"most_cases_country.txt")

