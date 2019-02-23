---
mathjax: include
title: k-Nearest Neighbors Join
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
Implements an exact k-nearest neighbors join algorithm.  Given a training set $A$ and a testing set $B$, the algorithm returns

$$
KNNJ(A, B, k) = \{ \left( b, KNN(b, A, k) \right) \text{ where } b \in B \text{ and } KNN(b, A, k) \text{ are the k-nearest points to }b\text{ in }A \}
$$

The brute-force approach is to compute the distance between every training and testing point. To ease the brute-force computation of computing the distance between every training point a quadtree is used. The quadtree scales well in the number of training points, though poorly in the spatial dimension. The algorithm will automatically choose whether or not to use the quadtree, though the user can override that decision by setting a parameter to force use or not use a quadtree.

## Operations

`KNN` is a `Predictor`.
As such, it supports the `fit` and `predict` operation.

### Fit

KNN is trained by a given set of `Vector`:

* `fit[T <: Vector]: DataSet[T] => Unit`

### Predict

KNN predicts for all subtypes of FlinkML's `Vector` the corresponding class label:

* `predict[T <: Vector]: DataSet[T] => DataSet[(T, Array[Vector])]`, where the `(T, Array[Vector])` tuple
  corresponds to (test point, K-nearest training points)

## Parameters

The KNN implementation can be controlled by the following parameters:

   <table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 20%">Parameters</th>
        <th class="text-center">Description</th>
      </tr>
    </thead>

    <tbody>
      <tr>
        <td><strong>K</strong></td>
        <td>
          <p>
            Defines the number of nearest-neighbors to search for. That is, for each test point, the algorithm finds the K-nearest neighbors in the training set
            (Default value: <strong>5</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>DistanceMetric</strong></td>
        <td>
          <p>
            Sets the distance metric we use to calculate the distance between two points. If no metric is specified, then [[org.apache.flink.ml.metrics.distances.EuclideanDistanceMetric]] is used.
            (Default value: <strong>EuclideanDistanceMetric</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>Blocks</strong></td>
        <td>
          <p>
            Sets the number of blocks into which the input data will be split. This number should be set
            at least to the degree of parallelism. If no value is specified, then the parallelism of the
            input [[DataSet]] is used as the number of blocks.
            (Default value: <strong>None</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>UseQuadTree</strong></td>
        <td>
          <p>
            A boolean variable that whether or not to use a quadtree to partition the training set to potentially simplify the KNN search. If no value is specified, the code will automatically decide whether or not to use a quadtree. Use of a quadtree scales well with the number of training and testing points, though poorly with the dimension.
            (Default value: <strong>None</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>SizeHint</strong></td>
        <td>
          <p>Specifies whether the training set or test set is small to optimize the cross product operation needed for the KNN search. If the training set is small this should be `CrossHint.FIRST_IS_SMALL` and set to `CrossHint.SECOND_IS_SMALL` if the test set is small.
             (Default value: <strong>None</strong>)
          </p>
        </td>
      </tr>
    </tbody>
  </table>

## Examples

{% highlight scala %}
import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint
import org.apache.flink.api.scala._
import org.apache.flink.ml.nn.KNN
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.metrics.distances.SquaredEuclideanDistanceMetric

val env = ExecutionEnvironment.getExecutionEnvironment

// prepare data
val trainingSet: DataSet[Vector] = ...
val testingSet: DataSet[Vector] = ...

val knn = KNN()
  .setK(3)
  .setBlocks(10)
  .setDistanceMetric(SquaredEuclideanDistanceMetric())
  .setUseQuadTree(false)
  .setSizeHint(CrossHint.SECOND_IS_SMALL)

// run knn join
knn.fit(trainingSet)
val result = knn.predict(testingSet).collect()
{% endhighlight %}

For more details on the computing KNN with and without and quadtree, here is a presentation: [http://danielblazevski.github.io/](http://danielblazevski.github.io/)

{% top %}
