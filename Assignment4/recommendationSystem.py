#Import Libaries
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkConf, SparkContext
import itertools
import sys
from operator import add
from math import sqrt

def RMSEcomputation(model, data, n):
    #Compute RMSE (Root Mean Squared Error).
    predicts = model.predictAll(data.map(lambda x: (x[0], x[1])))
    rateAndPredict = predicts.map(lambda x: ((x[0], x[1]), x[2])).join(data.map(lambda x: ((x[0], x[1]), x[2]))).values()
    return sqrt(rateAndPredict.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

if __name__ == "__main__":
    conf = SparkConf().setAppName("MovieLensALS").set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)

    # Load and parse the data
    data = sc.textFile("ratings.dat")
    rates = data.map(lambda l: l.strip().split('::')).map(
        lambda l: (float(l[3]) % 10, (int(l[0]), int(l[1]), float(l[2]))))


    numPartitions = 4
    training = rates.filter(lambda x: x[0] < 6).values().repartition(numPartitions).cache()
    validation = rates.filter(lambda x: x[0] >= 6 and x[0] < 8).values().repartition(numPartitions).cache()
    test = rates.filter(lambda x: x[0] >= 8).values().cache()

    trainNum = training.count()
    validateNum = validation.count()
    testNum = test.count()
    print("Training: %d, validation: %d, test: %d" %
        (trainNum, validateNum, testNum))

    ranks = [8, 12]
    lambdas = [0.1, 10.0]
    numIters = [10, 20]
    modelBest = None
    bestValRmse = float("inf")
    rankBest = 0
    lambdaBest = -1.0
    numCountBest = -1

    for rank, lambdaVal, numIter in itertools.product(ranks, lambdas, numIters):
        model = ALS.train(training, rank, numIter, lambdaVal)
        validationRmse = RMSEcomputation(model, validation, validateNum)
        print("RMSE (validation) = %f for the model trained with " % validationRmse +
            "rank = %d, lambda = %.1f, and numIter = %d." % (
                rank, lambdaVal, numIter))
        if (validationRmse < bestValRmse):
            modelBest = model
            bestValRmse = validationRmse
            rankBest = rank
            lambdaBest = lambdaVal
            numCountBest = numIter

        testRmse = RMSEcomputation(modelBest, test, testNum)

        print("The best model was trained with rank -> %d and lambda -> %.1f, " % (rankBest, lambdaBest)
            + "and with Iterations -> %d, the RMSE for the test set is %f." % (numCountBest, testRmse))
