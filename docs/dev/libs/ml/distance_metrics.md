---
mathjax: include
title: Distance Metrics
nav-parent_id: ml
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

Different metrics of distance are convenient for different types of analysis. Flink ML provides
built-in implementations for many standard distance metrics. You can create custom
distance metrics by implementing the `DistanceMetric` trait.

## Built-in Implementations

Currently, FlinkML supports the following metrics:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 20%">Metric</th>
        <th class="text-center">Description</th>
      </tr>
    </thead>

    <tbody>
      <tr>
        <td><strong>Euclidean Distance</strong></td>
        <td>
          $$d(\x, \y) = \sqrt{\sum_{i=1}^n \left(x_i - y_i \right)^2}$$
        </td>
      </tr>
      <tr>
        <td><strong>Squared Euclidean Distance</strong></td>
        <td>
          $$d(\x, \y) = \sum_{i=1}^n \left(x_i - y_i \right)^2$$
        </td>
      </tr>
      <tr>
        <td><strong>Cosine Similarity</strong></td>
        <td>
          $$d(\x, \y) = 1 - \frac{\x^T \y}{\Vert \x \Vert \Vert \y \Vert}$$
        </td>
      </tr>
      <tr>
        <td><strong>Chebyshev Distance</strong></td>
        <td>
          $$d(\x, \y) = \max_{i}\left(\left \vert x_i - y_i \right\vert \right)$$
        </td>
      </tr>
      <tr>
        <td><strong>Manhattan Distance</strong></td>
        <td>
          $$d(\x, \y) = \sum_{i=1}^n \left\vert x_i - y_i \right\vert$$
        </td>
      </tr>
      <tr>
        <td><strong>Minkowski Distance</strong></td>
        <td>
          $$d(\x, \y) = \left( \sum_{i=1}^{n} \left( x_i - y_i \right)^p \right)^{\rfrac{1}{p}}$$
        </td>
      </tr>
      <tr>
        <td><strong>Tanimoto Distance</strong></td>
        <td>
          $$d(\x, \y) = 1 - \frac{\x^T\y}{\Vert \x \Vert^2 + \Vert \y \Vert^2 - \x^T\y}$$
          with $\x$ and $\y$ being bit-vectors
        </td>
      </tr>
    </tbody>
  </table>

## Custom Implementation

You can create your own distance metric by implementing the `DistanceMetric` trait.

{% highlight scala %}
class MyDistance extends DistanceMetric {
  override def distance(a: Vector, b: Vector) = ... // your implementation for distance metric
}

object MyDistance {
  def apply() = new MyDistance()
}

val myMetric = MyDistance()
{% endhighlight %}

{% top %}
