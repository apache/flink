---
mathjax: include
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
familiar with Machine Learning (ML).

As defined by Murphy [1] ML deals with detecting patterns in data, and using those
learned patterns to make predictions about the future. We can categorize most ML algorithms into
two major categories: Supervised and Unsupervised Learning.

* Supervised Learning deals with learning a function (mapping) from a set of inputs
(features) to a set of outputs. The learning is done using a __training set__ of (input,
output) pairs that we use to approximate the mapping function. Supervised learning problems are
further divided into classification and regression problems. In classification problems we try to
predict the __class__ that an example belongs to, for example whether a user is going to click on
an ad or not. Regression problems one the other hand, are about predicting (real) numerical 
values, often called the dependent variable, for example what the temperature will be tomorrow.

* Unsupervised learning deals with discovering patterns and regularities in the data. An example
of this would be *clustering*, where we try to discover groupings of the data from the
descriptive features. Unsupervised learning can also be used for feature selection, for example
through [principal components analysis](https://en.wikipedia.org/wiki/Principal_component_analysis).

## Loading data

For loading data to be used with FlinkML we can use the ETL capabilities of Flink, or specialized
functions for formatted data, such as the LibSVM format. For supervised learning problems it is
common to use the `LabeledVector` class to represent the `(features, label)` examples. A `LabeledVector`
object will have a FlinkML `Vector` member representing the features of the example and a `Double`
member which represents the label, which could be the class in a classification problem, or the dependent
variable for a regression problem.

As an example, we can use Haberman's Survival Data Set , which you can
[download from the UCI ML repository](http://archive.ics.uci.edu/ml/machine-learning-databases/haberman/haberman.data.

We can load the data as a `DataSet[String]` first:

{% highlight scala %}

import org.apache.flink.api.scala.ExecutionEnvironment

val env = ExecutionEnvironment.createLocalEnvironment(2)

val survival = env.readCsvFile[(String, String, String, String)]("/path/to/haberman.data")

{% endhighlight %}

We can now transform the data into a `DataSet[LabeledVector]`. This will allow us to use the
dataset with the FlinkML classification algorithms. We know that the 4th element of the dataset
is the class label, and the rest are features, wo we can build `LabeledVector` elements like this:

{% highlight scala %}

import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

val survivalLV = survival
  .map{tuple =>
    val list = tuple.productIterator.toList
    val numList = list.map(_.asInstanceOf[String].toDouble)
    LabeledVector(numList(3), DenseVector(numList.take(3).toArray))
  }

{% endhighlight %}

We can then use this data to train a learner. We will use another dataset to exemplify building a
learner, that will allow us to show how we can import other dataset formats.

**LibSVM files**

A common format for ML datasets is the LibSVM format and a number of datasets using that format can be
found [in the LibSVM datasets website](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/). FlinkML provides utilities for loading
datasets using the LibSVM format through the `readLibSVM` function available through the MLUtils object.
You can also save datasets in the LibSVM format using the `writeLibSVM` function.
Let's import the svmguide1 dataset. You can download the 
[training set here](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/svmguide1)
and the [test set here](http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/svmguide1.t).

We can simply import the dataset then using:

{% highlight scala %}

import org.apache.flink.ml.MLUtils

val astroTrain = MLUtils.readLibSVM("path/to/svmguide1")
val astroTest = MLUtils.readLibSVM("path/to/svmguide1.t")

{% endhighlight %}

This gives us two `DataSet[LabeledVector]` that we will use in the following section to create a 
classifier.

## Classification

Once we have imported the dataset we can train a `Predictor` such as a linear SVM classifier.
We can set a number of parameters for the classifier. Here we set the `Blocks` parameter,
which is used to split the input by the underlying CoCoA algorithm [2] uses. The regularization
parameter determines the amount of $\lambda_2$ regularization applied, which is used
to avoid overfitting. The step size determines the contribution of the weight vector updates to 
the next weight vector value. The parameter sets the initial step size.

{% highlight scala %}

import org.apache.flink.ml.classification.SVM

val svm = SVM()
  .setBlocks(env.getParallelism)
  .setIterations(100)
  .setRegularization(0.001)
  .setStepsize(0.1)
  .setSeed(42)

svm.fit(astroTrain)

{% endhighlight %}

We can now make predictions on the test set.

{% highlight scala %}

val predictionPairs = svm.predict(astroTest)

{% endhighlight %}

Next we will see if we can improve the performance by pre-processing our data.

## Data pre-processing and pipelines

A pre-processing step that is often encouraged when using SVM classification is scaling
the input features to the [0, 1] range, in order to avoid features with extreme values dominating the rest.
FlinkML has a number of `Transformers` such as `MinMaxScaler` that are used to pre-process data, 
and a key feature is the ability to chain `Transformers` and `Predictors` together. This allows 
us to run the same pipeline of transformations and make predictions on the train and test data in
a straight-forward and type-safe manner. You can read more on the pipeline system of FlinkML
[in the pipelines documentation](pipelines.html).

Let us first create a normalizing transformer for the features in our dataset, and chain it to a 
new SVM classifier.

{% highlight scala %}

import org.apache.flink.ml.preprocessing.MinMaxScaler

val scaler = MinMaxScaler()

val scaledSVM = scaler.chainPredictor(svm)

{% endhighlight %}

We can now use our newly created pipeline to make predictions on the test set. 
First we call fit again, to train the scaler and the SVM classifier.
The data of the test set will then be automatically scaled before being passed on to the SVM to 
make predictions.

{% highlight scala %}

scaledSVM.fit(astroTrain)

val predictionPairsScaled= scaledSVM.predict(astroTest)

{% endhighlight %}

The scaled inputs should give us better prediction performance.

## Where to go from here

This quickstart guide can act as an introduction to the basic concepts of FlinkML, but there's a lot
more you can do.
We recommend going through the [FlinkML documentation](index.html), and trying out the different
algorithms.
A very good way to get started is to play around with interesting datasets from the UCI ML 
repository and the LibSVM datasets.
Tackling an interesting problem from a website like [Kaggle](https://www.kaggle.com) or 
[DrivenData](http://www.drivendata.org/) is also a great way to learn by competing with other 
data scientists.
If you would like to contribute some new algorithms take a look at our 
[contribution guide](contribution_guide.html).

**References**

[1] Murphy, Kevin P. *Machine learning: a probabilistic perspective.* MIT press, 2012.

[2] Jaggi, Martin, et al. *Communication-efficient distributed dual coordinate ascent.* 
Advances in Neural Information Processing Systems. 2014.
