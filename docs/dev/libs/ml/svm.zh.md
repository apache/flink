---
mathjax: include
title: SVM using CoCoA
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

Implements an SVM with soft-margin using the communication-efficient distributed dual coordinate
ascent algorithm with hinge-loss function.
The algorithm solves the following minimization problem:

$$\min_{\mathbf{w} \in \mathbb{R}^d} \frac{\lambda}{2} \left\lVert \mathbf{w} \right\rVert^2 + \frac{1}{n} \sum_{i=1}^n l_{i}\left(\mathbf{w}^T\mathbf{x}_i\right)$$

with $\mathbf{w}$ being the weight vector, $\lambda$ being the regularization constant,
$$\mathbf{x}_i \in \mathbb{R}^d$$ being the data points and $$l_{i}$$ being the convex loss
functions, which can also depend on the labels $$y_{i} \in \mathbb{R}$$.
In the current implementation the regularizer is the $\ell_2$-norm and the loss functions are the hinge-loss functions:

  $$l_{i} = \max\left(0, 1 - y_{i} \mathbf{w}^T\mathbf{x}_i \right)$$

With these choices, the problem definition is equivalent to a SVM with soft-margin.
Thus, the algorithm allows us to train a SVM with soft-margin.

The minimization problem is solved by applying stochastic dual coordinate ascent (SDCA).
In order to make the algorithm efficient in a distributed setting, the CoCoA algorithm calculates
several iterations of SDCA locally on a data block before merging the local updates into a
valid global state.
This state is redistributed to the different data partitions where the next round of local SDCA
iterations is then executed.
The number of outer iterations and local SDCA iterations control the overall network costs, because
there is only network communication required for each outer iteration.
The local SDCA iterations are embarrassingly parallel once the individual data partitions have been
distributed across the cluster.

The implementation of this algorithm is based on the work of
[Jaggi et al.](http://arxiv.org/abs/1409.1458)

## Operations

`SVM` is a `Predictor`.
As such, it supports the `fit` and `predict` operation.

### Fit

SVM is trained given a set of `LabeledVector`:

* `fit: DataSet[LabeledVector] => Unit`

### Predict

SVM predicts for all subtypes of FlinkML's `Vector` the corresponding class label:

* `predict[T <: Vector]: DataSet[T] => DataSet[(T, Double)]`, where the `(T, Double)` tuple
  corresponds to (original_features, label)

If we call evaluate with a `DataSet[(Vector, Double)]`, we make a prediction on the class label
for each example, and return a `DataSet[(Double, Double)]`. In each tuple the first element
is the true value, as was provided from the input `DataSet[(Vector, Double)]` and the second element
is the predicted value. You can then use these `(truth, prediction)` tuples to evaluate
the algorithm's performance.

* `predict: DataSet[(Vector, Double)] => DataSet[(Double, Double)]`

## Parameters

The SVM implementation can be controlled by the following parameters:

<table class="table table-bordered">
<thead>
  <tr>
    <th class="text-left" style="width: 20%">Parameters</th>
    <th class="text-center">Description</th>
  </tr>
</thead>

<tbody>
  <tr>
    <td><strong>Blocks</strong></td>
    <td>
      <p>
        Sets the number of blocks into which the input data will be split.
        On each block the local stochastic dual coordinate ascent method is executed.
        This number should be set at least to the degree of parallelism.
        If no value is specified, then the parallelism of the input DataSet is used as the number of blocks.
        (Default value: <strong>None</strong>)
      </p>
    </td>
  </tr>
  <tr>
    <td><strong>Iterations</strong></td>
    <td>
      <p>
        Defines the maximum number of iterations of the outer loop method.
        In other words, it defines how often the SDCA method is applied to the blocked data.
        After each iteration, the locally computed weight vector updates have to be reduced to update the global weight vector value.
        The new weight vector is broadcast to all SDCA tasks at the beginning of each iteration.
        (Default value: <strong>10</strong>)
      </p>
    </td>
  </tr>
  <tr>
    <td><strong>LocalIterations</strong></td>
    <td>
      <p>
        Defines the maximum number of SDCA iterations.
        In other words, it defines how many data points are drawn from each local data block to calculate the stochastic dual coordinate ascent.
        (Default value: <strong>10</strong>)
      </p>
    </td>
  </tr>
  <tr>
    <td><strong>Regularization</strong></td>
    <td>
      <p>
        Defines the regularization constant of the SVM algorithm.
        The higher the value, the smaller will the 2-norm of the weight vector be.
        In case of a SVM with hinge loss this means that the SVM margin will be wider even though it might contain some false classifications.
        (Default value: <strong>1.0</strong>)
      </p>
    </td>
  </tr>
  <tr>
    <td><strong>Stepsize</strong></td>
    <td>
      <p>
        Defines the initial step size for the updates of the weight vector.
        The larger the step size is, the larger will be the contribution of the weight vector updates to the next weight vector value.
        The effective scaling of the updates is $\frac{stepsize}{blocks}$.
        This value has to be tuned in case that the algorithm becomes unstable.
        (Default value: <strong>1.0</strong>)
      </p>
    </td>
  </tr>
  <tr>
    <td><strong>ThresholdValue</strong></td>
    <td>
      <p>
        Defines the limiting value for the decision function above which examples are labeled as
        positive (+1.0). Examples with a decision function value below this value are classified
        as negative (-1.0). In order to get the raw decision function values you need to indicate it by
        using the OutputDecisionFunction parameter.  (Default value: <strong>0.0</strong>)
      </p>
    </td>
  </tr>
  <tr>
    <td><strong>OutputDecisionFunction</strong></td>
    <td>
      <p>
        Determines whether the predict and evaluate functions of the SVM should return the distance
        to the separating hyperplane, or binary class labels. Setting this to true will
        return the raw distance to the hyperplane for each example. Setting it to false will
        return the binary class label (+1.0, -1.0) (Default value: <strong>false</strong>)
      </p>
    </td>
  </tr>
  <tr>
  <td><strong>Seed</strong></td>
  <td>
    <p>
      Defines the seed to initialize the random number generator.
      The seed directly controls which data points are chosen for the SDCA method.
      (Default value: <strong>Random Long Integer</strong>)
    </p>
  </td>
</tr>
</tbody>
</table>

## Examples

{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.RichExecutionEnvironment

val pathToTrainingFile: String = ???
val pathToTestingFile: String = ???
val env = ExecutionEnvironment.getExecutionEnvironment

// Read the training data set, from a LibSVM formatted file
val trainingDS: DataSet[LabeledVector] = env.readLibSVM(pathToTrainingFile)

// Create the SVM learner
val svm = SVM()
  .setBlocks(10)

// Learn the SVM model
svm.fit(trainingDS)

// Read the testing data set
val testingDS: DataSet[Vector] = env.readLibSVM(pathToTestingFile).map(_.vector)

// Calculate the predictions for the testing data set
val predictionDS: DataSet[(Vector, Double)] = svm.predict(testingDS)

{% endhighlight %}

{% top %}
