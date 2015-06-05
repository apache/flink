---
htmlTitle: FlinkML - Quickstart Guide
title: <a href="../ml">FlinkML</a> - Quickstart Guide
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}

## Introduction

FlinkML is designed to make learning from your data a straight-forward process, abstracting away
the complexities that usually come with having to deal with big data learning tasks. In this
quick-start guide we will show just how easy it is to solve a simple supervised learning problem
using FlinkML. But first some basics, feel free to skip the next few lines if you're already
familiar with Machine Learning (ML)

As defined by Murphy [cite ML-APP] ML deals with detecting patterns in data, and using those
learned patterns to make predictions about the future. We can categorize most ML algorithms into
two major categories: Supervised and Unsupervised Learning.

* Supervised Learning deals with learning a function (mapping) from a set of inputs
(predictors) to a set of outputs. The learning is done using a __training set__ of (input,
output) pairs that we use to approximate the mapping function. Supervised learning problems are
further divided into classification and regression problems. In classification problems we try to
predict the __class__ that an example belongs to, for example whether a user is going to click on
an ad or not. Regression problems are about predicting (real) numerical values,  often called the dependent
variable, for example what the temperature will be tomorrow.

* Unsupervised learning deals with discovering patterns and regularities in the data. An example
of this would be __clustering__, where we try to discover groupings of the data from the
descriptive features. Unsupervised learning can also be used for feature selection, for example
through [principal components analysis](https://en.wikipedia.org/wiki/Principal_component_analysis).

## Loading data

For loading data to be used with FlinkML we can use the ETL capabilities of Flink, or specialized
functions for formatted data, such as the LibSVM format. For supervised learning problems it is
common to use the `LabeledVector` class to represent the `(features, label)` examples. A `LabeledVector`
object will have a FlinkML `Vector` member representing the features of the example and a `Double`
member which represents the label, which could be the class in a classification problem, or the dependent
variable for a regression problem.

# TODO: Get dataset that has separate train and test sets
As an example, we can use the Breast Cancer Wisconsin (Diagnostic) Data Set, which you can
[download from the UCI ML repository](http://archive.ics.uci.edu/ml/machine-learning-databases/breast-cancer-wisconsin/breast-cancer-wisconsin.data).

We can open up a Flink shell and load the data as a `DataSet[String]` at first:

{% highlight scala %}

val cancer = env.readCsvFile[(String, String, String, String, String, String, String, String, String, String, String)]("/path/to/breast-cancer-wisconsin.data")

{% endhighlight %}

The dataset has some missing values indicated by `?`. We can filter those rows out and
then transform the data into a `DataSet[LabeledVector]`. This will allow us to use the
dataset with the FlinkML classification algorithms.

{% highlight scala %}

val cancerLV = cancer
  .map(_.productIterator.toList)
  .filter(!_.contains("?"))
  .map(_.foreach())//???

{% endhighlight %}

A common format for ML datasets is the LibSVM format and a number of datasets using that format can be
found [in the LibSVM datasets website](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/). FlinkML provides utilities for loading
datasets using the LibSVM format through the `readLibSVM` function available through the MLUtils object.
You can also save datasets in the LibSVM format using the `writeLibSVM` function.
Let's import the Adult (a9a) dataset. You can download the 
[training set here](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/a8a)
and the [test set here](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/a8a.t).

We can simply import the dataset then using:

{% highlight scala %}

val adultTrain = MLUtils.readLibSVM("path/to/a8a")
val adultTest = MLUtils.readLibSVM("path/to/a8a.t")

{% endhighlight %}

This gives us a `DataSet[LabeledVector]` that we will use in the following section to create a classifier.

Due to an error in the test dataset we have to adjust the test data using the following code, to 
ensure that the dimensionality of all test examples is 123, as with the training set:

{% highlight scala %}

val adjustedTest = adultTest.map{lv =>
      val vec = lv.vector.asBreeze
      val padded = vec.padTo(123, 0.0).toDenseVector
      val fvec = padded.fromBreeze
      LabeledVector(lv.label, fvec)
    }

{% endhighlight %}

## Classification

Once we have imported the dataset we can train a `Predictor` such as a linear SVM classifier.

{% highlight scala %}

val svm = SVM()
  .setBlocks(env.getParallelism)
  .setIterations(100)
  .setRegularization(0.001)
  .setStepsize(0.1)
  .setSeed(42)

svm.fit(adultTrain)

{% endhighlight %}

Let's now make predictions on the test set and see how well we do in terms of absolute error
We will also create a function that thresholds the predictions to the {-1, 1} scale that the
dataset uses.

{% highlight scala %}

def thresholdPredictions(predictions: DataSet[(Double, Double)])
: DataSet[(Double, Double)] = {
  predictions.map {
    truthPrediction =>
      val truth = truthPrediction._1
      val prediction = truthPrediction._2
      val thresholdedPrediction = if (prediction > 0.0) 1.0 else -1.0
      (truth, thresholdedPrediction)
  }
}

val predictionPairs = thresholdPredictions(svm.predict(adjustedTest))

val absoluteErrorSum = predictionPairs.collect().map{
  case (truth, prediction) => Math.abs(truth - prediction)}.sum

println(s"Absolute error: $absoluteErrorSum")

{% endhighlight %}

Next we will see if we can improve the performance by pre-processing our data.

## Data pre-processing and pipelines

A pre-processing step that is often encouraged when using SVM classification is scaling
the input features to the [0, 1] range, in order to avoid features with extreme values dominating the rest.
FlinkML has a number of `Transformers` such as `StandardScaler` that are used to pre-process data, and a key feature is the ability to
chain `Transformers` and `Predictors` together. This allows us to run the same pipeline of transformations and make predictions
on the train and test data in a straight-forward and type-safe manner. You can read more on the pipeline system of FlinkML,
[here](pipelines.html).

Let first create a scaling transformer for the features in our dataset, and chain it to a new SVM classifier.

{% highlight scala %}

import org.apache.flink.ml.preprocessing.StandardScaler

val scaler = StandardScaler()
scaler.fit(adultTrain)

val scaledSVM = scaler.chainPredictor(svm)

{% endhighlight %}

We can now use our newly created pipeline to make predictions on the test set. 
First we call fit again, to train the scaler and the SVM classifier.
The data of the test set will then be automatically scaled before being passed on to the SVM to 
make predictions.

{% highlight scala %}

scaledSVM.fit(adultTrain)

val predictionPairsScaled= thresholdPredictions(scaledSVM.predict(predictionsScaled))

val absoluteErrorSumScaled = predictionPairs.collect().map{
  case (truth, prediction) => Math.abs(truth - prediction)}.sum

println(s"Absolute error with scaled features: $absoluteErrorSumScaled")

{% endhighlight %}

The effect that the transformation has on the rror for this dataset is a bit unpredictable.
In reality the scaling transformation does
not fit the dataset we are using, since the features are translated categorical features and as
such, operations like normalization and standard scaling do not make much sense.

## Where to go from here

This quickstart guide can act as an introduction to the basic concepts of FlinkML, but there's a lot more you can do.
We recommend going through the [FlinkML documentation](index.html), and trying out the different algorithms.
A very good way to get started is to play around with interesting datasets from the UCI ML repository and the LibSVM datasets.
Tackling an interesting problem from a website like [Kaggle](https://www.kaggle.com) or [DrivenData](http://www.drivendata.org/)
is also a great way to learn by competing with other data scientists.
If you would like to contribute some new algorithms take a look at our [contribution guide](contribution_guide.html).
