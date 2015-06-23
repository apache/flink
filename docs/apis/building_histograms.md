---
title: "Building histograms"
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

Histograms are a very useful way to discover statistical properties of data. Flink supports two
types of histograms: *Discrete* and *Continuous*.

1. **Discrete Histogram**: These can be used to represent category-wise distribution of a
discrete valued data set. Following operations are supported:
  * `count(c)`: Number of elements belonging to category `c`
  * `entropy()`: Entropy of the data set in bits
  * `gini()`: Gini impurity of the data set

2. **Continuous Histogram**: These can be used to approximately represent a continuous valued
data set. A continuous histogram distributes the data set into *bins*, which are specified by the
user at the time of construction. Note that a higher number of bins results in a more accurate
representation of data, but takes more space and time to maintain.
A continuous histogram supports the following operations:
  * `count(s)`: Count the number of elements less than `s`.
  * `quantile(q)`: Find the *q*th quantile of the data. Here, *q*th quantile represents a value `v`
  such that *q* fraction of elements are less than `v`.
  * `min`, `max`, `mean`, `variance`: Respective statistics of the underlying data set.

This document shows how {% gh_link /flink-java/src/main/java/org/apache/flink/api/java/utils/DataSetUtils.java "DataSetUtils" %} can be used for building these histograms.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.api.common.accumulators.DiscreteHistogram;
import org.apache.flink.api.common.accumulators.ContinuousHistogram;
import org.apache.flink.api.java.utils.DataSetUtils;

ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
DataSet<Double> data = env.fromElements(1.0, 2.0, 2.0, 3.0);

DiscreteHistogram discreteHistogram = DataSetUtils.createDiscreteHistogram(data).collect().get(0);
discreteHistogram.count(1.0);  // equals 1
discreteHistogram.count(2.0); // equals 2
discreteHistogram.entropy();  // equals 1.5
discreteHistogram.gini();    // equals 5/8

ContinuousHistogram continuousHistogram = DataSetUtils.createContinuousHistogram(data, 2).collect().get(0);
continuousHistogram.count(2.0); // equals 2 (approximately)
continuousHistogram.quantile(0.5); // 1.9, i.e. 2 elements are less than 1.9
continuousHistogram.min();  // equals 1.0
continuousHistogram.max();  // equals 3.0
continuousHistogram.mean();  // equals 2.0
continuousHistogram.variance();  // equals 0.5
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._

val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
val data: DataSet[Double] = env.fromElements(1.0, 2.0, 2.0, 3.0)

val discreteHistogram = data.createDiscreteHistogram.collect().head
discreteHistogram.count(1.0)  // equals 1
discreteHistogram.count(2.0) // equals 2
discreteHistogram.entropy()  // equals 1.5
discreteHistogram.gini()    // equals 5/8

val continuousHistogram = data.createContinuousHistogram(2).collect().head
continuousHistogram.count(2.0) // equals 2 (approximately)
continuousHistogram.quantile(0.5) // 1.9, i.e. 2 elements are less than 1.9
continuousHistogram.min();  // equals 1.0
continuousHistogram.max();  // equals 3.0
continuousHistogram.mean();  // equals 2.0
continuousHistogram.variance();  // equals 0.5
{% endhighlight %}
</div>
</div>
[Back to top](#top)