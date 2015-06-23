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

 The Statistics package provides methods to evaluate statistics over `DataSet` of `Vector`.

 The following information is available as part of `DataStats`:
 1. Number of elements in `X`
 2. Dimension of `X`
 3. Field-wise statistics:
   * For Discrete fields: Category wise counts, Entropy, Gini impurity
   * For Continuous fields: Minimum, Maximum, Mean, Variance

## Methods

 The `dataStats` function operates on a data set `X: DataSet[Vector]` and returns  statistics for
 `X`. Every field of `X` is allowed to be defined as either *discrete* or *continuous*.

 Statistics can be evaluated by calling `X.dataStats()` or `X.dataStats(discreteFields)`. The
 latter is used when some fields need to be declared discrete-valued, and is provided as an
 array of indices of discrete fields.

 `dataStats` also verifies the validity of data by making sure all elements have the exact same
 dimension.

## Examples

{% highlight scala %}

import org.apache.flink.ml._

// Evaluate Data set statistics
val dataSet: DataSet[Vector] = ...

// evaluate statistics considering the 0th and 2nd field as discrete
val stats = dataSet.dataStats(Array(0,2)).collect.head

stats.total                             // total number of elements in dataSet
stats.dimension                         // dimension of vectors in dataSet
stats.fields(0).entropy                 // entropy of the 0th attribute
stats.fields(2).categoryCounts          // hashmap of (category, count) for the 2nd attribute
stats.fields(3).variance                // variance of the 3rd attribute
{% endhighlight %}
