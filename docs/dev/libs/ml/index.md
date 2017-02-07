---
title: "FlinkML - Machine Learning for Flink"
nav-id: ml
nav-show_overview: true
nav-title: Machine Learning
nav-parent_id: libs
nav-pos: 4
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

FlinkML is the Machine Learning (ML) library for Flink. It is a new effort in the Flink community,
with a growing list of algorithms and contributors. With FlinkML we aim to provide
scalable ML algorithms, an intuitive API, and tools that help minimize glue code in end-to-end ML
systems. You can see more details about our goals and where the library is headed in our [vision
and roadmap here](https://cwiki.apache.org/confluence/display/FLINK/FlinkML%3A+Vision+and+Roadmap).

* This will be replaced by the TOC
{:toc}

## Supported Algorithms

FlinkML currently supports the following algorithms:

### Supervised Learning

* [SVM using Communication efficient distributed dual coordinate ascent (CoCoA)](svm.html)
* [Multiple linear regression](multiple_linear_regression.html)
* [Optimization Framework](optimization.html)

### Unsupervised Learning

* [k-Nearest neighbors join](knn.html)

### Data Preprocessing

* [Polynomial Features](polynomial_features.html)
* [Standard Scaler](standard_scaler.html)
* [MinMax Scaler](min_max_scaler.html)

### Recommendation

* [Alternating Least Squares (ALS)](als.html)

### Outlier selection

* [Stochastic Outlier Selection (SOS)](sos.html)

### Utilities

* [Distance Metrics](distance_metrics.html)
* [Cross Validation](cross_validation.html)

## Getting Started

You can check out our [quickstart guide](quickstart.html) for a comprehensive getting started
example.

If you want to jump right in, you have to [set up a Flink program]({{ site.baseurl }}/dev/linking_with_flink.html).
Next, you have to add the FlinkML dependency to the `pom.xml` of your project.

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-ml{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that FlinkML is currently not part of the binary distribution.
See linking with it for cluster execution [here]({{site.baseurl}}/dev/linking.html).

Now you can start solving your analysis task.
The following code snippet shows how easy it is to train a multiple linear regression model.

{% highlight scala %}
// LabeledVector is a feature vector with a label (class or real value)
val trainingData: DataSet[LabeledVector] = ...
val testingData: DataSet[Vector] = ...

// Alternatively, a Splitter is used to break up a DataSet into training and testing data.
val dataSet: DataSet[LabeledVector] = ...
val trainTestData: DataSet[TrainTestDataSet] = Splitter.trainTestSplit(dataSet)
val trainingData: DataSet[LabeledVector] = trainTestData.training
val testingData: DataSet[Vector] = trainTestData.testing.map(lv => lv.vector)

val mlr = MultipleLinearRegression()
  .setStepsize(1.0)
  .setIterations(100)
  .setConvergenceThreshold(0.001)

mlr.fit(trainingData)

// The fitted model can now be used to make predictions
val predictions: DataSet[LabeledVector] = mlr.predict(testingData)
{% endhighlight %}

## Pipelines

A key concept of FlinkML is its [scikit-learn](http://scikit-learn.org) inspired pipelining mechanism.
It allows you to quickly build complex data analysis pipelines how they appear in every data scientist's daily work.
An in-depth description of FlinkML's pipelines and their internal workings can be found [here](pipelines.html).

The following example code shows how easy it is to set up an analysis pipeline with FlinkML.

{% highlight scala %}
val trainingData: DataSet[LabeledVector] = ...
val testingData: DataSet[Vector] = ...

val scaler = StandardScaler()
val polyFeatures = PolynomialFeatures().setDegree(3)
val mlr = MultipleLinearRegression()

// Construct pipeline of standard scaler, polynomial features and multiple linear regression
val pipeline = scaler.chainTransformer(polyFeatures).chainPredictor(mlr)

// Train pipeline
pipeline.fit(trainingData)

// Calculate predictions
val predictions: DataSet[LabeledVector] = pipeline.predict(testingData)
{% endhighlight %}

One can chain a `Transformer` to another `Transformer` or a set of chained `Transformers` by calling the method `chainTransformer`.
If one wants to chain a `Predictor` to a `Transformer` or a set of chained `Transformers`, one has to call the method `chainPredictor`.


## How to contribute

The Flink community welcomes all contributors who want to get involved in the development of Flink and its libraries.
In order to get quickly started with contributing to FlinkML, please read our official
[contribution guide]({{site.baseurl}}/dev/libs/ml/contribution_guide.html).
