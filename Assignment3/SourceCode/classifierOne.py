# BY - Tanushri Singh
# CS 6350.001 - Big Data Management and Analytics
# Instructor - Latifur Khan
# Assignment 3

# Import Libraries
import pandas as pd
import numpy as np
from collections import Counter
import re
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.svm import LinearSVC
from sklearn.multiclass import OneVsRestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score,confusion_matrix

# Read in file
df=pd.read_csv('guardian.csv')
print(df.sample(10))
print(Counter(df["category"]))

# preprocessing the data
def cleanFile(string):
    string=re.sub(r"\n","",string)
    string=re.sub(r"\r","",string)
    string=re.sub(r"[0-9]","",string)
    string=re.sub(r"\"","",string)
    string=re.sub(r"\'","",string)
    return string.strip().lower()

X=[]
for i in range(df.shape[0]):
    X.append(cleanFile(df.iloc[i][0]))

y=np.array(df["category"])

# This is to split training and testing data
trainX,testX,trainY,testY=train_test_split(X,y,test_size=0.1,random_state=5)
model=Pipeline([('vectorizer',CountVectorizer()),('tfidf',TfidfTransformer()),
('clf',OneVsRestClassifier(LinearSVC(class_weight="balanced")))])

# Use Vectors and then GridSearchCV
parameters = {'vectorizer__ngram_range': [(1, 1), (1, 2),(2,2)],
               'tfidf__use_idf': (True, False)}
gridSearchSVM = GridSearchCV(model, parameters, n_jobs=-1)
gridSearchSVM = gridSearchSVM.fit(X, y)
print(gridSearchSVM.best_score_)
print(gridSearchSVM.best_params_)

# To Fit the model with optimal parameters
model=Pipeline([('vectorizer',CountVectorizer(ngram_range=(1,1))),('tfidf',TfidfTransformer(use_idf=True)),
('clf',OneVsRestClassifier(LinearSVC(class_weight="balanced")))])

# In order to fit model with training data
model.fit(trainX,trainY)

# prediction on whole data set and Output results
predictionOfTrainData=model.predict(trainX)
print('Training accuracy -> %s'%accuracy_score(predictionOfTrainData,trainY))
testDataPrediction=model.predict(testX)
print('Testing data accuracy -> %s'%accuracy_score(testDataPrediction,testY))
testDataPredictionModel=model.predict(X)
print('Overall data accuracy -> %s'%accuracy_score(testDataPredictionModel,y))
