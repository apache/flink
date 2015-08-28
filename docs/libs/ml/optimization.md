---
mathjax: include
htmlTitle: FlinkML - Optimization
title: <a href="../ml">FlinkML</a> - Optimization
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

* Table of contents
{:toc}

## Mathematical Formulation

The optimization framework in FlinkML is a developer-oriented package that can be used to solve
[optimization](https://en.wikipedia.org/wiki/Mathematical_optimization)
problems common in Machine Learning (ML) tasks. In the supervised learning context, this usually
involves finding a model, as defined by a set of parameters $w$, that minimize a function $f(\wv)$
given a set of $(\x, y)$ examples,
where $\x$ is a feature vector and $y$ is a real number, which can represent either a real value in
the regression case, or a class label in the classification case. In supervised learning, the
function to be minimized is usually of the form:


\begin{equation} \label{eq:objectiveFunc}
    f(\wv) :=
    \frac1n \sum_{i=1}^n L(\wv;\x_i,y_i) +
    \lambda\, R(\wv)
    \ .
\end{equation}


where $L$ is the loss function and $R(\wv)$ the regularization penalty. We use $L$ to measure how
well the model fits the observed data, and we use $R$ in order to impose a complexity cost to the
model, with $\lambda > 0$ being the regularization parameter.

### Loss Functions

In supervised learning, we use loss functions in order to measure the model fit, by
penalizing errors in the predictions $p$ made by the model compared to the true $y$ for each
example. Different loss functions can be used for regression (e.g. Squared Loss) and classification
(e.g. Hinge Loss) tasks.

Some common loss functions are:

* Squared Loss: $ \frac{1}{2} \left(\wv^T \cdot \x - y\right)^2, \quad y \in \R $
* Hinge Loss: $ \max \left(0, 1 - y ~ \wv^T \cdot \x\right), \quad y \in \{-1, +1\} $
* Logistic Loss: $ \log\left(1+\exp\left( -y ~ \wv^T \cdot \x\right)\right), \quad y \in \{-1, +1\}$

### Regularization Types

[Regularization](https://en.wikipedia.org/wiki/Regularization_(mathematics)) in machine learning
imposes penalties to the estimated models, in order to reduce overfitting. The most common penalties
are the $L_1$ and $L_2$ penalties, defined as:

* $L_1$: $R(\wv) = \norm{\wv}_1$
* $L_2$: $R(\wv) = \frac{1}{2}\norm{\wv}_2^2$

The $L_2$ penalty penalizes large weights, favoring solutions with more small weights rather than
few large ones.
The $L_1$ penalty can be used to drive a number of the solution coefficients to 0, thereby
producing sparse solutions.
The regularization constant $\lambda$ in $\eqref{eq:objectiveFunc}$ determines the amount of regularization applied to the model,
and is usually determined through model cross-validation. 
A good comparison of regularization types can be found in [this](http://www.robotics.stanford.edu/~ang/papers/icml04-l1l2.pdf) paper by Andrew Ng.
Which regularization type is supported depends on the actually used optimization algorithm.

## Stochastic Gradient Descent

In order to find a (local) minimum of a function, Gradient Descent methods take steps in the
direction opposite to the gradient of the function $\eqref{eq:objectiveFunc}$ taken with
respect to the current parameters (weights).
In order to compute the exact gradient we need to perform one pass through all the points in
a dataset, making the process computationally expensive.
An alternative is Stochastic Gradient Descent (SGD) where at each iteration we sample one point
from the complete dataset and update the parameters for each point, in an online manner.

In mini-batch SGD we instead sample random subsets of the dataset, and compute the gradient
over each batch. At each iteration of the algorithm we update the weights once, based on
the average of the gradients computed from each mini-batch.

An important parameter is the learning rate $\eta$, or step size, which is currently determined as
$\eta = \eta_0/\sqrt{j}$, where $\eta_0$ is the initial step size and $j$ is the iteration
number. The setting of the initial step size can significantly affect the performance of the
algorithm. For some practical tips on tuning SGD see Leon Botou's
"[Stochastic Gradient Descent Tricks](http://research.microsoft.com/pubs/192769/tricks-2012.pdf)".

The current implementation of SGD  uses the whole partition, making it
effectively a batch gradient descent. Once a sampling operator has been introduced in Flink, true
mini-batch SGD will be performed.

### Regularization

FlinkML supports Stochastic Gradient Descent with L1, L2 and no regularization.
The following list contains a mapping between the implementing classes and the regularization function.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Class Name</th>
      <th class="text-center">Regularization function $R(\wv)$</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>SimpleGradient</code></td>
      <td>$R(\wv) = 0$</td>
    </tr>
    <tr>
      <td><code>GradientDescentL1</code></td>
      <td>$R(\wv) = \norm{\wv}_1$</td>
    </tr>
    <tr>
      <td><code>GradientDescentL2</code></td>
      <td>$R(\wv) = \frac{1}{2}\norm{\wv}_2^2$</td>
    </tr>
  </tbody>
</table>

### Parameters

  The stochastic gradient descent implementation can be controlled by the following parameters:

   <table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 20%">Parameter</th>
        <th class="text-center">Description</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>LossFunction</strong></td>
        <td>
          <p>
            The loss function to be optimized. (Default value: <strong>None</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>RegularizationConstant</strong></td>
        <td>
          <p>
            The amount of regularization to apply. (Default value: <strong>0.0</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>Iterations</strong></td>
        <td>
          <p>
            The maximum number of iterations. (Default value: <strong>10</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>LearningRate</strong></td>
        <td>
          <p>
            Initial learning rate for the gradient descent method.
            This value controls how far the gradient descent method moves in the opposite direction
            of the gradient.
            (Default value: <strong>0.1</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>ConvergenceThreshold</strong></td>
        <td>
          <p>
            When set, iterations stop if the relative change in the value of the objective function $\eqref{eq:objectiveFunc}$ is less than the provided threshold, $\tau$.
            The convergence criterion is defined as follows: $\left| \frac{f(\wv)_{i-1} - f(\wv)_i}{f(\wv)_{i-1}}\right| < \tau$.
            (Default value: <strong>None</strong>)
          </p>
        </td>
      </tr>
    </tbody>
  </table>
  
### Loss Function

The loss function which is minimized has to implement the `LossFunction` interface, which defines methods to compute the loss and the gradient of it.
Either one defines ones own `LossFunction` or one uses the `GenericLossFunction` class which constructs the loss function from an outer loss function and a prediction function.
An example can be seen here

```Scala
val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction) 
```

The full list of supported outer loss functions can be found [here](#partial-loss-function-values).
The full list of supported prediction functions can be found [here](#prediction-function-values).
  
#### Partial Loss Function Values ##

  <table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 20%">Function Name</th>
        <th class="text-center">Description</th>
        <th class="text-center">Loss</th>
        <th class="text-center">Loss Derivative</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>SquaredLoss</strong></td>
        <td>
          <p>
            Loss function most commonly used for regression tasks.
          </p>
        </td>
        <td class="text-center">$\frac{1}{2} (\wv^T \cdot \x - y)^2$</td>
        <td class="text-center">$\wv^T \cdot \x - y$</td>
      </tr>
    </tbody>
  </table>

#### Prediction Function Values ##

  <table class="table table-bordered">
      <thead>
        <tr>
          <th class="text-left" style="width: 20%">Function Name</th>
          <th class="text-center">Description</th>
          <th class="text-center">Prediction</th>
          <th class="text-center">Prediction Gradient</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td><strong>LinearPrediction</strong></td>
          <td>
            <p>
              The function most commonly used for linear models, such as linear regression and
              linear classifiers.
            </p>
          </td>
          <td class="text-center">$\x^T \cdot \wv$</td>
          <td class="text-center">$\x$</td>
        </tr>
      </tbody>
    </table>

### Examples

In the Flink implementation of SGD, given a set of examples in a `DataSet[LabeledVector]` and
optionally some initial weights, we can use `GradientDescentL1.optimize()` in order to optimize
the weights for the given data.

The user can provide an initial `DataSet[WeightVector]`,
which contains one `WeightVector` element, or use the default weights which are all set to 0.
A `WeightVector` is a container class for the weights, which separates the intercept from the
weight vector. This allows us to avoid applying regularization to the intercept.



{% highlight scala %}
// Create stochastic gradient descent solver
val sgd = GradientDescentL1()
  .setLossFunction(SquaredLoss())
  .setRegularizationConstant(0.2)
  .setIterations(100)
  .setLearningRate(0.01)


// Obtain data
val trainingDS: DataSet[LabeledVector] = ...

// Optimize the weights, according to the provided data
val weightDS = sgd.optimize(trainingDS)
{% endhighlight %}
