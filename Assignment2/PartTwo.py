#	By: Tanushri Singh
#   CS 6350 - Big Data Management and Analytics
#  	Instructor: Latifur Khan

###################
#WHAT IT SHOULD DO:-
#Find top-10 friend pairs by their total number of common friends. For each top-10 friend pair
#print detail information in decreasing order of total number of common friends. More
#specifically the output format can be:
#<Total number of Common Friends><TAB><First Name of User A><TAB><Last Name of
#User A> <TAB><address of User A><TAB><First Name of User B><TAB><Last Name of
#User B><TAB><address of User B>
###################

#Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc
from pyspark.sql.types import ArrayType, StringType, IntegerType

#Create instance of spark and friend parser
spark = SparkSession.builder.getOrCreate()
parseFriendsUdf = udf(lambda stringFriends:
                      [] if stringFriends is None else
                      [int(friend) for friend in stringFriends.split(",")],
                      ArrayType(IntegerType(), False))

#Read in soc-LiveJournal file
friendDF = spark.read \
    .option("sep", "\t") \
    .csv("DataFiles/soc-LiveJournal1Adj.txt") \
    .withColumnRenamed("_c0", "userId") \
    .withColumnRenamed("_c1", "friends")
friendDF = friendDF \
    .withColumn("userId", friendDF["userId"].cast(IntegerType())) \
    .withColumn("friends", parseFriendsUdf(friendDF.friends))

#Read in userdata file
userInfoDF = spark.read \
    .csv("DataFiles/userdata.txt") \
    .withColumnRenamed("_c0", "userId") \
    .withColumnRenamed("_c1", "firstName") \
    .withColumnRenamed("_c2", "lastName") \
    .withColumnRenamed("_c3", "address") \
    .withColumnRenamed("_c4", "city") \
    .withColumnRenamed("_c5", "state") \
    .withColumnRenamed("_c6", "zipCode") \
    .withColumnRenamed("_c7", "country") \
    .withColumnRenamed("_c8", "userName") \
    .withColumnRenamed("_c9", "birthday")
userInfoDF = userInfoDF \
    .withColumn("userId", userInfoDF["userId"].cast(IntegerType()))

#Check dataframes are set up properly
print(friendDF.printSchema())
print(friendDF.show())
print(userInfoDF.printSchema())
print(userInfoDF.show())

# Function to make pairs
def makePairs(userId, friends):
    pairs = []
    for friend in friends:
        if userId < friend:
            pairs.append([userId, friend])
    return pairs

makePairsUdf = udf(makePairs, ArrayType(
    ArrayType(IntegerType(), False), False))

#Make new dataframe pairedDF to hold pair of friends
pairedDF = friendDF.sort("userId") \
    .withColumn("pairs", makePairsUdf(friendDF.userId, friendDF.friends)) \
    .select("pairs").rdd \
    .flatMap(lambda x: x) \
    .flatMap(lambda x: x) \
    .toDF()

#Rename columns of pairedDF
pairedDF = pairedDF \
    .withColumnRenamed("_1", "user1") \
    .withColumnRenamed("_2", "user2")
pairedDF = friendDF.select(col("userId"), col("friends").alias("user1friends")) \
    .join(pairedDF, friendDF.userId == pairedDF.user1, how="right")
pairedDF = friendDF.select(col("userId"), col("friends").alias("user2friends")) \
    .join(pairedDF, friendDF.userId == pairedDF.user2, how="right")

#Join to see common friends
commonFriendCountUdf = udf(lambda a, b: len(
    set(a).intersection(b)), IntegerType())
pairedDF = pairedDF \
    .withColumn("commonFriendsCount", commonFriendCountUdf(pairedDF.user1friends, pairedDF.user2friends))
pairedDF = userInfoDF.select("userId", col("firstName").alias("firstName1"), col("lastName").alias("lastName1"), col("address").alias("address1")) \
    .join(pairedDF, userInfoDF.userId == pairedDF.user1, how="right")
pairedDF = userInfoDF.select("userId", col("firstName").alias("firstName2"), col("lastName").alias("lastName2"), col("address").alias("address2")) \
    .join(pairedDF, userInfoDF.userId == pairedDF.user2, how="right")

#Check dataframes are set up properly
print(pairedDF.printSchema())
print(pairedDF.show())

#Compose output that is to be written, get top 10
output = pairedDF \
    .select("user1", "user2", "commonFriendsCount", "firstName1", "lastName1", "address1", "firstName2", "lastName2", "address2") \
    .sort(desc("commonFriendsCount")) \
    .take(10)

#Write output to file
outputFile = open("PartTwo_output.txt", "w")
for row in output:
    outputFile.write(str(row["commonFriendsCount"]) + "\t"
                     + row["firstName1"] + "\t"
                     + row["lastName1"] + "\t"
                     + row["address1"] + "\t"
                     + row["firstName2"] + "\t"
                     + row["lastName2"] + "\t"
                     + row["address2"] + "\n")
outputFile.close()
