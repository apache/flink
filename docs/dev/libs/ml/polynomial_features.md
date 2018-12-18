---
mathjax: include
title: Polynomial Features
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

The polynomial features transformer maps a vector into the polynomial feature space of degree $d$.
The dimension of the input vector determines the number of polynomial factors whose values are the respective vector entries.
Given a vector $(x, y, z, \ldots)^T$ the resulting feature vector looks like:

$$\left(x, y, z, x^2, xy, y^2, yz, z^2, x^3, x^2y, x^2z, xy^2, xyz, xz^2, y^3, \ldots\right)^T$$

Flink's implementation orders the polynomials in decreasing order of their degree.

Given the vector $\left(3,2\right)^T$, the polynomial features vector of degree 3 would look like

 $$\left(3^3, 3^2\cdot2, 3\cdot2^2, 2^3, 3^2, 3\cdot2, 2^2, 3, 2\right)^T$$

This transformer can be prepended to all `Transformer` and `Predictor` implementations which expect an input of type `LabeledVector` or any sub-type of `Vector`.

## Operations

`PolynomialFeatures` is a `Transformer`.
As such, it supports the `fit` and `transform` operation.

### Fit

PolynomialFeatures is not trained on data and, thus, supports all types of input data.

### Transform

PolynomialFeatures transforms all subtypes of `Vector` and `LabeledVector` into their respective types:

* `transform[T <: Vector]: DataSet[T] => DataSet[T]`
* `transform: DataSet[LabeledVector] => DataSet[LabeledVector]`

## Parameters

The polynomial features transformer can be controlled by the following parameters:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 20%">Parameters</th>
        <th class="text-center">Description</th>
      </tr>
    </thead>

    <tbody>
      <tr>
        <td><strong>Degree</strong></td>
        <td>
          <p>
            The maximum polynomial degree.
            (Default value: <strong>10</strong>)
          </p>
        </td>
      </tr>
    </tbody>
  </table>

## Examples

{% highlight scala %}
// Obtain the training data set
val trainingDS: DataSet[LabeledVector] = ...

// Setup polynomial feature transformer of degree 3
val polyFeatures = PolynomialFeatures()
.setDegree(3)

// Setup the multiple linear regression learner
val mlr = MultipleLinearRegression()

// Control the learner via the parameter map
val parameters = ParameterMap()
.add(MultipleLinearRegression.Iterations, 20)
.add(MultipleLinearRegression.Stepsize, 0.5)

// Create pipeline PolynomialFeatures -> MultipleLinearRegression
val pipeline = polyFeatures.chainPredictor(mlr)

// train the model
pipeline.fit(trainingDS)
{% endhighlight %}

{% top %}
