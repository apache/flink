---
mathjax: include
htmlTitle: FlinkML - Statistics
title: <a href="../ml">FlinkML</a> - Statistics
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

## Description

 The statistics utility provides features such as building histograms over data, determining
 mean, variance, gini impurity, entropy etc. of data.

## Methods

 The Statistics utility provides two major functions: `createHistogram` and `dataStats`.

### Creating a histogram

 There are two types of histograms:
   1. **Continuous Histograms**: These histograms are formed on a data set `X: DataSet[Double]`
   when the values in `X` are from a continuous range. These histograms support
   `quantile` and `sum`  operations. Here `quantile(q)` refers to a value $x_q$ such that $|x: x
   \leq x_q| = q * |X|$. Further, `sum(s)` refers to the number of elements $x \leq s$, which can
    be construed as a cumulative probability value at $s$[Of course, *scaled* probability].
   2. A continuous histogram can be formed by calling `X.createHistogram(b)` where `b` is the
    number of bins.
    **Discrete Histograms**: These histograms are formed on a data set `X:DataSet[Double]`
    when the values in `X` are from a discrete distribution. These histograms
    support `count(c)` operation which returns the number of elements associated with cateogry `c`.
    <br>
        A discrete histogram can be formed by calling `MLUtils.createDiscreteHistogram(X)`.

### Data Statistics

 The `dataStats` function operates on a data set `X: DataSet[Vector]` and returns column-wise
 statistics for `X`. Every field of `X` is allowed to be defined as either *discrete* or
 *continuous*.
 <br>
 Statistics can be evaluated by calling `DataStats.dataStats(X)` or
 `DataStats.dataStats(X, discreteFields)`. The latter is used when some fields are needed to be
 declared discrete-valued, and is provided as an array of indices of fields which are discrete.
 <br>
 The following information is available as part of `DataStats`:
    1. Number of elements in `X`
    2. Dimension of `X`
    3. Column-wise statistics where for discrete fields, we report counts for each category, and
     the Gini impurity and Entropy of the field, while for continuous fields, we report the
     minimum, maximum, mean and variance.

## Examples

{% highlight scala %}

import org.apache.flink.ml.statistics._
import org.apache.flink.ml._

val X: DataSet[Double] = ...
// Create continuous histogram
val histogram = X.createHistogram(5).collect.head     // creates a histogram with five bins
histogram.quantile(0.3)                  // returns the 30th quantile
histogram.sum(4)                         // returns number of elements less than 4

// Create discrete histogram
val histogram = MLUtils.createDiscreteHistogram(X).collect.head     // creates a discrete histogram
histogram.count(3)                       // number of elements with category value 3

// Evaluate Data set statistics
val dataSet: DataSet[Vector] = ...

// evaluate statistics considering the 0th and 2nd field as discrete
val stats = DataStats.dataStats(dataSet, Array(0,2)).collect.head

stats.total                             // total number of elements in dataSet
stats.dimension                         // dimension of vectors in dataSet
stats.fields(0).entropy                 // entropy of the 0th attribute
stats.fields(2).categoryCounts          // hashmap of (category, count) for the 2nd attribute
stats.fields(3).variance                // variance of the 3rd attribute
{% endhighlight %}
