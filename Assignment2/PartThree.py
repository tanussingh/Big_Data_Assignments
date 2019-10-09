#	By: Tanushri Singh
#   CS 6350 - Big Data Management and Analytics
#  	Instructor: Latifur Khan

###################
#WHAT IT SHOULD DO:-
#List the 'user id' and 'rating' of users that reviewed businesses classified as “Colleges &
#Universities” in list of categories.
#Required files are 'business' and 'review'.
#Sample output:-
#User id Rating
#0WaCdhr3aXb0G0niwTMGTg 4.0
###################

#Import Lib
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, array_contains
from pyspark.sql.types import ArrayType, StringType

#Initialize spark
spark = SparkSession.builder.getOrCreate()

#Read in business.csv
businessDF = spark.read \
    .option("sep", ":") \
    .csv("DataFiles/business.csv") \
    .withColumnRenamed("_c0", "businessID") \
    .withColumnRenamed("_c2", "address") \
    .withColumnRenamed("_c4", "categories") \
    .drop("_c1") \
    .drop("_c3")
categoryParseUdf = udf(
    lambda x: x[5:-1].split(", "),
    ArrayType(StringType(), False))
businessDF = businessDF \
    .withColumn("categories", categoryParseUdf(businessDF.categories))

#read in review.csv
reviewDF = spark.read \
    .option("sep", ":") \
    .csv("DataFiles/review.csv") \
    .withColumnRenamed("_c0", "reviewId") \
    .withColumnRenamed("_c2", "userId") \
    .withColumnRenamed("_c4", "businessID") \
    .withColumnRenamed("_c6", "stars") \
    .drop("_c1") \
    .drop("_c3") \
    .drop("_c5")

#Check dataframes are set up properly
print(businessDF.printSchema())
print(businessDF.show())
print(reviewDF.printSchema())
print(reviewDF.show())

#Create Join based on businessID
reviewDF = businessDF \
    .select("businessID", "categories") \
    .join(reviewDF, businessDF.businessID == reviewDF.businessID, how="right")

#Aggregate output
output = reviewDF \
    .filter(array_contains(reviewDF.categories, "Colleges & Universities")) \
    .select("userId", "stars") \
    .collect()

#Write Out Output
outputFile = open("PartThree_output.txt", "w")
for row in output:
    outputFile.write(row["userId"] + "\t"
                     + row["stars"] + "\n")

outputFile.close()
