---
mathjax: include
title: Normalizer
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

The Normalizer scales the given data set, so that all samples will have a unit measure based on the norm p provided by the user.
Given a set of input data vectors $x_1, x_2,... x_n$, and norm p of a vector $v$ defined as:

 $$\|v\|_p=(\sum_{i=1}^{n} v_i^{p})^{1/p}$$

The scaled data set $z_1, z_2,...,z_n$ will be:

 $$z_{i}=\frac{x_i}{\|x_i\|_p}$$$

where $p$ is the user provided norm value.

## Operations

`Normalizer` is a `Transformer`.
As such, it supports the `fit` and `transform` operation.

### Fit

Normalizer is not trained on data and, thus, supports all types of input data.

### Transform

Normalizer transforms all subtypes of `Vector` or `LabeledVector` into the respective type:

* `transform[T <: Vector]: DataSet[T] => DataSet[T]`
* `transform: DataSet[LabeledVector] => DataSet[LabeledVector]`

## Parameters

The normalizer implementation can be controlled by the following parameter:

 <table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Parameters</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>p</strong></td>
      <td>
        <p>
          The p norm value to use for the normalization. (Default value: <strong>2.0</strong>)
        </p>
      </td>
    </tr>
  </tbody>
</table>

## Examples

{% highlight scala %}
import org.apache.flink.ml.math.Norm

// Create standard normalizer transformer
val normalizer = Normalizer().setNorm(Norm.L1)

// Obtain data set to be normalized per sample
val dataSet: DataSet[Vector] = ...

// Normalize the provided data set to have measure 1.0
val normalizedDS = normalizer.transform(dataSet)
{% endhighlight %}
