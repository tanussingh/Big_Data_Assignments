#	By: Tanushri Singh
#   CS 6350 - Big Data Management and Analytics
#  	Instructor: Latifur Khan

###################
#WHAT IT SHOULD DO:-
#List the business_id , full address and categories of the Top 10 businesses located in “NY”
#using the average ratings.
#This will require you to use review.csv and business.csv files.
#Sample output:
#business id, full address, categories, avg rating
#xdf12344444444, CA 91711 List['Local Services', 'Carpet Cleaning'] 5.0
###################

#Import Lib
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, array_contains, mean, desc
from pyspark.sql.types import ArrayType, StringType

#Read in file, rename lists and clean up data
spark = SparkSession.builder.getOrCreate()
#businessDF file
businessDF = spark.read.option("sep", ":").csv("DataFiles/business.csv")
businessDF = businessDF \
        .withColumnRenamed('_c0', 'businessID') \
        .withColumnRenamed('_c2', 'address') \
        .withColumnRenamed('_c4', 'categories')
businessDF = businessDF \
        .drop('_c1') \
        .drop('_c3')

categoryParseUdf = udf(lambda stringCategories:
                       stringCategories[5:-1].split(", "),
                       ArrayType(StringType(), False))
stateParseUdf = udf(lambda address:
                    address.split(" ")[-2],
                    StringType())
businessDF = businessDF \
    .withColumn("categories", categoryParseUdf(businessDF.categories)) \
    .withColumn("state", stateParseUdf(businessDF.address))

#review file
reviewDF = spark.read.option("sep", ":").csv("DataFiles/review.csv")
reviewDF = reviewDF \
        .withColumnRenamed('_c0', 'reviewID') \
        .withColumnRenamed('_c2', 'userID') \
        .withColumnRenamed('_c4', 'businessID') \
        .withColumnRenamed('_c6', 'rating')
reviewDF = reviewDF \
        .drop('_c1') \
        .drop('_c3') \
        .drop('_c5')

#Check dataframes are set up properly
print(businessDF.printSchema())
print(businessDF.show())
print(reviewDF.printSchema())
print(reviewDF.show())

#Get all the businesses located in NY
#Sort based on rating for these businesses
reviewDF = businessDF \
    .join(reviewDF, "businessID", how="right")
reviewDF = reviewDF \
    .filter(reviewDF.state == "NY") \
    .groupBy(reviewDF.businessID, reviewDF.address, reviewDF.categories) \
    .agg(mean("rating").alias("averageRating"))
output = reviewDF \
    .sort(desc("averageRating")) \
    .take(10)

#Output business id, full address, categories, rating of TOP 10, use .head
outputFile = open("PartFour_output.txt", "w")
for row in output:
    outputFile.write(row["businessID"] + "\t"
                     + row["address"] + "\t"
                     + str(row["categories"]) + "\t"
                     + str(row["averageRating"]) + "\n")

outputFile.close()
