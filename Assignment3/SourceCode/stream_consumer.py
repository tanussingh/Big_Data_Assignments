# BY - Tanushri Singh
# CS 6350.001 - Big Data Management and Analytics
# Instructor - Latifur Khan
# Assignment 3

# COMPRESS:- $ kafka-console-consumer --bootstrap-server localhost:9092
# --topic guardian2 --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import csv
import pandas as pd

print("Consumer is now reading from Kafka")
consume = KafkaConsumer(
    'guardian2',
     bootstrap_servers=['localhost:9092'],
     value_deserializer=lambda x: loads(x.decode('utf-8')))
print("Consumer is now beginning to write to file!")
val=1
with open('guardian.csv', mode='w') as guardian:
    writer = csv.writer(guardian, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['text', 'category'])
    for message in consume:
            p=message.value.split("||")
            writer.writerow([p[1]+" "+p[2], p[0]])
            guardian.flush()
            print("Message received successfully. Message Number "+str(val))
            val=val+1
    print("Consumer has finished reading from Kafka and Writing to Localfile.")
