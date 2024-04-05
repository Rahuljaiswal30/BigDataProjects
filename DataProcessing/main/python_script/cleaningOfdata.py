#This script is for learning purpose
#Use case: Transform and clean the data and convert it into the tabular format for Analytical purpose
# Author : Rahul Jaiswal
# Date : 4th April, 2024


#Import all the required libraries here
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql types import StructType, StructField, StringType, IntegerType
from pyspark.sql functions import *
from pyspark.sql.functions import regex_repace, col

#create a dataframe
df = spark.read.load('/file/path/of/the/csvFile.csv', format='csv', sep=',', header='trus', escape='"', inferschema='true')


#we can check the df if it is creaed successfully or not. by usinf some of below mentioned function( we have many more out as well)
df.count()  #prints the total number of the records
df.show(1) #prints only the first record
df.printSchema() #used to check the schema of the df created


#Data cleaning steps
#1. lets say we want to delete some of the columns from the data frame which is not requied for the further analysis.
df=df.drop("size","content Rating", "Last Updated", "Android ver", "Current ver") #all the coluumn mentioned inside the bracket will be deleted from the df
#print the updated dataframe
df.show(2)


#lets modify the schema as per our requirements
df=df.withColumn("Reviews", col("Reviews").cast(IntegerType()))\
.withColumn("Installs",regex_repace(col("Install"),"[^0-9]",""))\
.withColumn("Installs",col("Installs").cast(IntegerType()))\
.withColumn("Price",regex_repace(col("Price"),"[$]",""))\
.withColumn("Price",col("price").cast(IntegerType()))

#now the data cleaning and transforming is done lets start doing analysis as per the requirement.
#to do the analysis we need to convert the dataframe into View
df.createOrReplaceTempView("apps")

#we can use the simple sql query to do the analysis.

#1. Top reviews given to the app.
### Select App, sum(Reviews) from apps group by 1 order by 2 desc;

#2.Top 10 installs app and distribution
### Select App, sum(install) from apps group by 1 order by 2 desc;

#3. Category wise distrebution 
### select category, sum(installs) from app group by 1 order by 2 desc

#4. Top paid apps
### Select Apps, sum(price) from Apps where type='paid' group by 1 order by 2 desc 
#since this project was done in Databricks 