# Big_Data_Assignments
Usage of Apache Hadoop, Spark and Kafka. 

## Assignment 1
Utilizies hadoop and mapreduce to analyze social network data. 
Input files that are utilized include soc-LiveJournal1Adj.txt and userdata.txt both of which are included in the folder. Calculations are performed based on various techniques such as In-Memory Join etc.

## Assignment 2
Utilizes spark and its inbuilt functionalities to solve various types of problems. Once again soc-LiveJournal1Adj.txt and userdata.txt are utlized to perform calculations and address the given problems. Statistics are also derived on statistics from a Yelp Dataset that are presented in the following text files: Business.csv, review.csv and user.csv

## Assignment 3
The focus is on extracting features and building a classifier over a stream of news articles. Goal is to gather real time news articles using streaming tool that is provided by Guardian API. In order to use the . API one must first create an API key using the platform. Then Kafka streaming is used to render data every 1 second. Now a standalone spark and kafka system must be built in order to perfom calculations on said data. In order to accomplish this pipeline model with tokenizers and stopword removers are utilized. Then a TF-IDF vectorizer and classifiers are used to calculate appropriate results.

## Assignment 4 
This assignment was ML based. It addresses techniques such as Clustering (K-means Algorithm, hierarchical clustering etc), Classification (Decision Trees) and Programming a CNN. I worked on the MNIST dataset and applied PyTorch packages to implement the CNN algorithm. The model was trained to predict labels which were later tested. 

The structure followed the following patter:
Convolutional layer -> Max pooling layer -> Convolutional layer - > Max pooling layer -> Fully connected layer x2 -> Softmax layer
