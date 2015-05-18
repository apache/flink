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

## Getting Started

You can use FlinkML in your project by adding the following dependency to your pom.xml

{% highlight bash %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-ml</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

## Supported Algorithms

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

## Example & Quickstart guide

We already have some of the building blocks for FlinkML in place, and will continue to extend the
library with more algorithms. An example of how simple it is to create a learning model in
FlinkML is given below:

{% highlight scala %}
// LabeledVector is a feature vector with a label (class or real value)
val data: DataSet[LabeledVector] = ...

val learner = MultipleLinearRegression()
  .setStepsize(1.0)
  .setIterations(100)
  .setConvergenceThreshold(0.001)

learner.fit(data, parameters)

// The learner can now be used to make predictions using learner.predict()
{% endhighlight %}

For a more comprehensive guide, you can check out our [quickstart guide](quickstart.html)

## How to contribute

Please check our [roadmap](vision_roadmap.html#roadmap) and [contribution guide](contribution_guide.html). 
You can also check out our list of
[unresolved issues on JIRA](https://issues.apache.org/jira/browse/FLINK-1748?jql=component%20%3D%20%22Machine%20Learning%20Library%22%20AND%20project%20%3D%20FLINK%20AND%20resolution%20%3D%20Unresolved%20ORDER%20BY%20priority%20DESC)
