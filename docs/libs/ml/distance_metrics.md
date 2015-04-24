---
title: Distance Metrics
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

Different metrics of distance are convenient for different types of analysis. Flink Machine
Learning Library provides built-in implementations for many standard distance metric. You can also
use other distance metric by implementing `DistanceMetric` trait.

## Built-in Implementations

* Euclidean Distance
* Squared Euclidean Distance
* Cosine Distance
* Tanimoto Distance
* Chebyshev Distance
* Manhattan Distance
* Minkowski Distance

## Custom Implementation

You can use your own distance metric by implementing `DistanceMetric` trait.

{% highlight scala %}
class MyDistance extends DistanceMetric {
  override def distance(a: Vector, b: Vector) = ... // your implementation for distance metric
}

object MyDistance {
  def apply() = new MyDistance()
}

val metric = MyDistance()
{% endhighlight %}
