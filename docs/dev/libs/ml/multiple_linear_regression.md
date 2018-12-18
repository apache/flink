---
mathjax: include
title: Multiple Linear Regression
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

 Multiple linear regression tries to find a linear function which best fits the provided input data.
 Given a set of input data with its value $(\mathbf{x}, y)$, multiple linear regression finds
 a vector $\mathbf{w}$ such that the sum of the squared residuals is minimized:

 $$ S(\mathbf{w}) = \sum_{i=1} \left(y - \mathbf{w}^T\mathbf{x_i} \right)^2$$

 Written in matrix notation, we obtain the following formulation:

 $$\mathbf{w}^* = \arg \min_{\mathbf{w}} (\mathbf{y} - X\mathbf{w})^2$$

 This problem has a closed form solution which is given by:

  $$\mathbf{w}^* = \left(X^TX\right)^{-1}X^T\mathbf{y}$$

  However, in cases where the input data set is so huge that a complete parse over the whole data
  set is prohibitive, one can apply stochastic gradient descent (SGD) to approximate the solution.
  SGD first calculates for a random subset of the input data set the gradients. The gradient
  for a given point $\mathbf{x}_i$ is given by:

  $$\nabla_{\mathbf{w}} S(\mathbf{w}, \mathbf{x_i}) = 2\left(\mathbf{w}^T\mathbf{x_i} -
    y\right)\mathbf{x_i}$$

  The gradients are averaged and scaled. The scaling is defined by $\gamma = \frac{s}{\sqrt{j}}$
  with $s$ being the initial step size and $j$ being the current iteration number. The resulting gradient is subtracted from the
  current weight vector giving the new weight vector for the next iteration:

  $$\mathbf{w}_{t+1} = \mathbf{w}_t - \gamma \frac{1}{n}\sum_{i=1}^n \nabla_{\mathbf{w}} S(\mathbf{w}, \mathbf{x_i})$$

  The multiple linear regression algorithm computes either a fixed number of SGD iterations or terminates based on a dynamic convergence criterion.
  The convergence criterion is the relative change in the sum of squared residuals:

  $$\frac{S_{k-1} - S_k}{S_{k-1}} < \rho$$

## Operations

`MultipleLinearRegression` is a `Predictor`.
As such, it supports the `fit` and `predict` operation.

### Fit

MultipleLinearRegression is trained on a set of `LabeledVector`:

* `fit: DataSet[LabeledVector] => Unit`

### Predict

MultipleLinearRegression predicts for all subtypes of `Vector` the corresponding regression value:

* `predict[T <: Vector]: DataSet[T] => DataSet[(T, Double)]`

## Parameters

  The multiple linear regression implementation can be controlled by the following parameters:

   <table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 20%">Parameters</th>
        <th class="text-center">Description</th>
      </tr>
    </thead>

    <tbody>
      <tr>
        <td><strong>Iterations</strong></td>
        <td>
          <p>
            The maximum number of iterations. (Default value: <strong>10</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>Stepsize</strong></td>
        <td>
          <p>
            Initial step size for the gradient descent method.
            This value controls how far the gradient descent method moves in the opposite direction of the gradient.
            Tuning this parameter might be crucial to make it stable and to obtain a better performance.
            (Default value: <strong>0.1</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>ConvergenceThreshold</strong></td>
        <td>
          <p>
            Threshold for relative change of the sum of squared residuals until the iteration is stopped.
            (Default value: <strong>None</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>LearningRateMethod</strong></td>
        <td>
            <p>
                Learning rate method used to calculate the effective learning rate for each iteration.
                See the list of supported <a href="optimization.html">learning rate methods</a>.
                (Default value: <strong>LearningRateMethod.Default</strong>)
            </p>
        </td>
      </tr>
    </tbody>
  </table>

## Examples

{% highlight scala %}
// Create multiple linear regression learner
val mlr = MultipleLinearRegression()
.setIterations(10)
.setStepsize(0.5)
.setConvergenceThreshold(0.001)

// Obtain training and testing data set
val trainingDS: DataSet[LabeledVector] = ...
val testingDS: DataSet[Vector] = ...

// Fit the linear model to the provided data
mlr.fit(trainingDS)

// Calculate the predictions for the test data
val predictions = mlr.predict(testingDS)
{% endhighlight %}

{% top %}
