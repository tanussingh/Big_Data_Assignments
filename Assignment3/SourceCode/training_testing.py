# BY - Tanushri Singh
# CS 6350.001 - Big Data Management and Analytics
# Instructor - Latifur Khan
# Assignment 3

# Import files
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC

#Read in from csv file
df = pd.read_csv('guardian2.csv')

col = [ 'text','category']
df = df[col]
df = df[pd.notnull(df['text'])]
df['category_id'] = df['category'].factorize()[0]
categoryIdDataFrame = df[['category', 'category_id']].drop_duplicates().sort_values('category_id')
categoryID = dict(categoryIdDataFrame.values)

#Initialize TFID Vector
tfidf = TfidfVectorizer(sublinear_tf=True, min_df=5, norm='l2',
encoding='latin-1', ngram_range=(1, 2), stop_words='english')
features = tfidf.fit_transform(df.text).toarray()
labels = df.category_id
print("----------------- SHAPE OF FEATURE -----------------")
print(features.shape)

#Setup initialial values for training and testing
trainX, testX, trainY, testY = train_test_split(df['text'], df['category'], random_state = 0)
countVector = CountVectorizer()
trainXCounts = countVector.fit_transform(trainX)
tfidfTransformer = TfidfTransformer()
tfidfTrainX = tfidfTransformer.fit_transform(trainXCounts)

#Conduct training and testing for each model
models = [LinearSVC(), LogisticRegression(random_state=0)]
CV = 5
tempDf = pd.DataFrame(index=range(CV * len(models)))
entries = []
for model in models:
  modelName = model.__class__.__name__
  accuracies = cross_val_score(model, features, labels, scoring='accuracy', cv=CV)
  for foldIndex, accuracy in enumerate(accuracies):
    entries.append((modelName, foldIndex, accuracy))
tempDf = pd.DataFrame(entries, columns=['modelName', 'foldIndex', 'accuracy'])

#Output Results
print("------------------- Individual Results -------------------")
print(tempDf.groupby('modelName').accuracy.mean())
print("------------------- Overall Result -------------------")
print(tempDf.mean())
