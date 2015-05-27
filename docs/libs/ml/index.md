---
title: "FlinkML - Machine Learning for Flink"
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
and roadmap here](vision_roadmap.html).

* This will be replaced by the TOC
{:toc}

## Supported Algorithms

FlinkML currently supports the following algorithms:

### Supervised Learning

* [Communication efficient distributed dual coordinate ascent (CoCoA)](cocoa.html)
* [Multiple linear regression](multiple_linear_regression.html)
* [Optimization Framework](optimization.html)

### Data Preprocessing

* [Polynomial Base Feature Mapper](polynomial_base_feature_mapper.html)
* [Standard Scaler](standard_scaler.html)

### Recommendation

* [Alternating Least Squares (ALS)](als.html)

### Utilities

* [Distance Metrics](distance_metrics.html)

## Getting Started

First, you have to [set up a Flink program](http://ci.apache.org/projects/flink/flink-docs-master/apis/programming_guide.html#linking-with-flink).
Next, you have to add the FlinkML dependency to the `pom.xml` of your project.  

{% highlight bash %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-ml</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Now you can start defining your ML pipelines.
The following code snippet shows how easy it is to train a multiple linear regression model.

{% highlight scala %}
// LabeledVector is a feature vector with a label (class or real value)
val trainingData: DataSet[LabeledVector] = ...
val testingData: DataSet[Vector] = ...

val mlr = MultipleLinearRegression()
  .setStepsize(1.0)
  .setIterations(100)
  .setConvergenceThreshold(0.001)

mlr.fit(trainingData, parameters)

// The fitted model can now be used to make predictions
val predictions: DataSet[LabeledVector] = mlr.predict(testingData)
{% endhighlight %}

For a more comprehensive guide, please check out our [quickstart guide](quickstart.html)

## Pipelines

A key concept of FlinkML is its [scikit-learn](http://scikit-learn.org) inspired pipelining mechanism.
It allows you to quickly build complex data analysis pipelines how they appear in every data scientist's daily work.

The following example code shows how easy it is to set up an analysis pipeline with FlinkML.

{% highlight scala %}
val trainingData: DataSet[LabeledVector] = ...
val testingData: DataSet[Vector] = ...

val scaler = StandardScaler()
val polyFeatures = PolynomialFeatures().setDegree(3)
val mlr = MultipleLinearRegression()

// Construct pipeline
val pipeline = scaler.chainTransformer(polyFeatures).chainPredictor(mlr)

// Train pipeline
pipeline.fit(trainingData)

// Calculate predictions
val predictions: DataSet[LabeledVector] = pipeline.predict(testingData)
{% endhighlight %} 

An in-depth description of FlinkML's pipelines and their internal workings can be found [here](pipelines.html)

## How to contribute

Please check our [roadmap](vision_roadmap.html#roadmap) and [contribution guide](contribution_guide.html). 
You can also check out our list of
[unresolved issues on JIRA](https://issues.apache.org/jira/browse/FLINK-1748?jql=component%20%3D%20%22Machine%20Learning%20Library%22%20AND%20project%20%3D%20FLINK%20AND%20resolution%20%3D%20Unresolved%20ORDER%20BY%20priority%20DESC)
