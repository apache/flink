---
mathjax: include
title: Standard Scaler
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

 The standard scaler scales the given data set, so that all features will have a user specified mean and variance.
 In case the user does not provide a specific mean and standard deviation, the standard scaler transforms the features of the input data set to have mean equal to 0 and standard deviation equal to 1.
 Given a set of input data $x_1, x_2,... x_n$, with mean:

 $$\bar{x} = \frac{1}{n}\sum_{i=1}^{n}x_{i}$$

 and standard deviation:

 $$\sigma_{x}=\sqrt{ \frac{1}{n} \sum_{i=1}^{n}(x_{i}-\bar{x})^{2}}$$

The scaled data set $z_1, z_2,...,z_n$ will be:

 $$z_{i}= std \left (\frac{x_{i} - \bar{x}  }{\sigma_{x}}\right ) + mean$$

where $\textit{std}$ and $\textit{mean}$ are the user specified values for the standard deviation and mean.

## Operations

`StandardScaler` is a `Transformer`.
As such, it supports the `fit` and `transform` operation.

### Fit

StandardScaler is trained on all subtypes of `Vector` or `LabeledVector`:

* `fit[T <: Vector]: DataSet[T] => Unit`
* `fit: DataSet[LabeledVector] => Unit`

### Transform

StandardScaler transforms all subtypes of `Vector` or `LabeledVector` into the respective type:

* `transform[T <: Vector]: DataSet[T] => DataSet[T]`
* `transform: DataSet[LabeledVector] => DataSet[LabeledVector]`

## Parameters

The standard scaler implementation can be controlled by the following two parameters:

 <table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Parameters</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>Mean</strong></td>
      <td>
        <p>
          The mean of the scaled data set. (Default value: <strong>0.0</strong>)
        </p>
      </td>
    </tr>
    <tr>
      <td><strong>Std</strong></td>
      <td>
        <p>
          The standard deviation of the scaled data set. (Default value: <strong>1.0</strong>)
        </p>
      </td>
    </tr>
  </tbody>
</table>

## Examples

{% highlight scala %}
// Create standard scaler transformer
val scaler = StandardScaler()
.setMean(10.0)
.setStd(2.0)

// Obtain data set to be scaled
val dataSet: DataSet[Vector] = ...

// Learn the mean and standard deviation of the training data
scaler.fit(dataSet)

// Scale the provided data set to have mean=10.0 and std=2.0
val scaledDS = scaler.transform(dataSet)
{% endhighlight %}

{% top %}
