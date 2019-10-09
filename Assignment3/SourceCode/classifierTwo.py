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
from sklearn.linear_model import LogisticRegression
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

# Inorder to split training and testing data
trainX,testX,trainY,testY=train_test_split(X,y,test_size=0.2,random_state=5)

# pipleline using SVM
model=Pipeline([('vectorizer',CountVectorizer(ngram_range=(1,1))),('tfidf',TfidfTransformer(use_idf=True)),
('clf',LogisticRegression(n_jobs=1,C=1e5))])

# Use vectorizer__ngram_range
parameters = {'vectorizer__ngram_range': [(1, 1), (1, 2),(2,2)],
               'tfidf__use_idf': (True, False)}
gs_clf_svm = GridSearchCV(model, parameters, n_jobs=-1)
gs_clf_svm = gs_clf_svm.fit(X, y)
print(gs_clf_svm.best_score_)
print(gs_clf_svm.best_params_)

# To fit model with the training data
model.fit(trainX,trainY)

# predicting on the whole dataset and outputting it
predictionOfTrainData=model.predict(trainX)
print('Training accuracy -> %s'%accuracy_score(predictionOfTrainData,trainY))
predictionOfTestData=model.predict(testX)
print('Testing data accuracy -> %s'%accuracy_score(predictionOfTestData,testY))
predictionOfDataModel=model.predict(X)
print('Overall data accuracy -> %s'%accuracy_score(predictionOfDataModel,y))
