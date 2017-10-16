---
mathjax: include
title: MinMax Scaler
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

 The MinMax scaler scales the given data set, so that all values will lie between a user specified range [min,max].
 In case the user does not provide a specific minimum and maximum value for the scaling range, the MinMax scaler transforms the features of the input data set to lie in the [0,1] interval.
 Given a set of input data $x_1, x_2,... x_n$, with minimum value:

 $$x_{min} = min({x_1, x_2,..., x_n})$$

 and maximum value:

 $$x_{max} = max({x_1, x_2,..., x_n})$$

The scaled data set $z_1, z_2,...,z_n$ will be:

 $$z_{i}= \frac{x_{i} - x_{min}}{x_{max} - x_{min}} \left ( max - min \right ) + min$$

where $\textit{min}$ and $\textit{max}$ are the user specified minimum and maximum values of the range to scale.

## Operations

`MinMaxScaler` is a `Transformer`.
As such, it supports the `fit` and `transform` operation.

### Fit

MinMaxScaler is trained on all subtypes of `Vector` or `LabeledVector`:

* `fit[T <: Vector]: DataSet[T] => Unit`
* `fit: DataSet[LabeledVector] => Unit`

### Transform

MinMaxScaler transforms all subtypes of `Vector` or `LabeledVector` into the respective type:

* `transform[T <: Vector]: DataSet[T] => DataSet[T]`
* `transform: DataSet[LabeledVector] => DataSet[LabeledVector]`

## Parameters

The MinMax scaler implementation can be controlled by the following two parameters:

 <table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Parameters</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>Min</strong></td>
      <td>
        <p>
          The minimum value of the range for the scaled data set. (Default value: <strong>0.0</strong>)
        </p>
      </td>
    </tr>
    <tr>
      <td><strong>Max</strong></td>
      <td>
        <p>
          The maximum value of the range for the scaled data set. (Default value: <strong>1.0</strong>)
        </p>
      </td>
    </tr>
  </tbody>
</table>

## Examples

{% highlight scala %}
// Create MinMax scaler transformer
val minMaxscaler = MinMaxScaler()
  .setMin(-1.0)

// Obtain data set to be scaled
val dataSet: DataSet[Vector] = ...

// Learn the minimum and maximum values of the training data
minMaxscaler.fit(dataSet)

// Scale the provided data set to have min=-1.0 and max=1.0
val scaledDS = minMaxscaler.transform(dataSet)
{% endhighlight %}

{% top %}
