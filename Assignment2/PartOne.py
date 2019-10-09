#	By: Tanushri Singh
#   CS 6350 - Big Data Management and Analytics
#  	Instructor: Latifur Khan

###################
#WHAT IT SHOULD DO:-
#The output should contain one line per user in the following format:
#<User_A>, <User_B><TAB><Mutual/Common Friend Number>
#where <User_A> & <User_B> are unique IDs corresponding to a user A and B (A and B are friend).
#< Mutual/Common Friend Number > is total number of common friends between user A and user B.
###################

#Import Lib
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()

#Read in soc-LiveJournal
inputDf = spark.read \
    .option("sep", "\t") \
    .csv("DataFiles/soc-LiveJournal1Adj.txt")
parseUdf = udf(lambda stringFriends:
               [] if stringFriends is None else
               [int(friend) for friend in stringFriends.split(",")],
               ArrayType(IntegerType(), False))
inputDf = inputDf \
    .withColumnRenamed("_c0", "userId") \
    .withColumnRenamed("_c1", "stringFriends")
inputDf = inputDf.withColumn("userId", inputDf["userId"].cast(IntegerType())) \
    .withColumn("friends", parseUdf(inputDf.stringFriends))

#Make pairs
def makePairs(userId, friends):
    pairs = []
    for friend in friends:
        if userId < friend:
            pairs.append([userId, friend])
    return pairs

makePairsUdf = udf(makePairs, ArrayType(
    ArrayType(IntegerType(), False), False))

#Make new Dataframe called pairedDf that stores just the pair of frieds
pairedDf = inputDf.sort("userId") \
    .withColumn("pairs", makePairsUdf(inputDf.userId, inputDf.friends)) \
    .select("pairs").rdd \
    .flatMap(lambda x: x) \
    .flatMap(lambda x: x) \
    .toDF()

#Rename columns of pairedDf
pairedDf = pairedDf \
    .withColumnRenamed("_1", "user1") \
    .withColumnRenamed("_2", "user2")
pairedDf = inputDf.select(col("userId"), col("friends").alias("user1friends")) \
    .join(pairedDf, inputDf.userId == pairedDf.user1, how="right")
pairedDf = inputDf.select(col("userId"), col("friends").alias("user2friends")) \
    .join(pairedDf, inputDf.userId == pairedDf.user2, how="right")
commonUdf = udf(lambda a, b: len(set(a).intersection(b)), IntegerType())

#Accumalate output
output = pairedDf.select("user1", "user2", "user1friends", "user2friends") \
    .withColumn("commonFriendsCount", commonUdf(pairedDf.user1friends, pairedDf.user2friends)) \
    .select("user1", "user2", "commonFriendsCount") \
    .orderBy("user1", "user2").collect()

#Output results to file
outputFile = open("PartOne_output.txt", "w")
for row in output:
    outputFile.write(str(row["user1"]) + ", " + str(row["user2"]) +
                     "\t" + str(row["commonFriendsCount"]) + "\n")
outputFile.close()
