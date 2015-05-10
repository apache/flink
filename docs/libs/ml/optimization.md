---
mathjax: include
title: "ML - Optimization"
displayTitle: <a href="index.md">ML</a> - Optimization
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

$$
\newcommand{\R}{\mathbb{R}}
\newcommand{\E}{\mathbb{E}} 
\newcommand{\x}{\mathbf{x}}
\newcommand{\y}{\mathbf{y}}
\newcommand{\wv}{\mathbf{w}}
\newcommand{\av}{\mathbf{\alpha}}
\newcommand{\bv}{\mathbf{b}}
\newcommand{\N}{\mathbb{N}}
\newcommand{\id}{\mathbf{I}}
\newcommand{\ind}{\mathbf{1}} 
\newcommand{\0}{\mathbf{0}} 
\newcommand{\unit}{\mathbf{e}} 
\newcommand{\one}{\mathbf{1}} 
\newcommand{\zero}{\mathbf{0}}
$$

## Mathematical Formulation

The optimization framework in Flink is a developer-oriented package that can be used to solve
[optimization](https://en.wikipedia.org/wiki/Mathematical_optimization) 
problems common in Machine Learning (ML) tasks. In the supervised learning context, this usually 
involves finding a model, as defined by a set of parameters $w$, that minimize a function $f(\wv)$ 
given a set of $(\x, y)$ examples,
where $\x$ is a feature vector and $y$ is a real number, which can represent either a real value in 
the regression case, or a class label in the classification case. In supervised learning, the 
function to be minimized is usually of the form:

$$
\begin{equation}
    f(\wv) := 
    \frac1n \sum_{i=1}^n L(\wv;\x_i,y_i) +
    \lambda\, R(\wv)
    \label{eq:objectiveFunc}
    \ .
\end{equation}
$$

where $L$ is the loss function and $R(\wv)$ the regularization penalty. We use $L$ to measure how
well the model fits the observed data, and we use $R$ in order to impose a complexity cost to the
model, with $\lambda > 0$ being the regularization parameter.

### Loss Functions

In supervised learning, we use loss functions in order to measure the model fit, by 
penalizing errors in the predictions $p$ made by the model compared to the true $y$ for each 
example. Different loss function can be used for regression (e.g. Squared Loss) and classification
(e.g. Hinge Loss).

Some common loss functions are:
 
* Squared Loss: $ \frac{1}{2} (\wv^T \x - y)^2, \quad y \in \R $ 
* Hinge Loss: $ \max (0, 1 - y ~ \wv^T \x), \quad y \in \{-1, +1\} $
* Logistic Loss: $ \log(1+\exp( -y ~ \wv^T \x)), \quad y \in \{-1, +1\} $

Currently, only the Squared Loss function is implemented in Flink.

### Regularization Types

[Regularization](https://en.wikipedia.org/wiki/Regularization_(mathematics)) in machine learning is 
imposes penalties to the estimated models, in order to reduce overfitting. The most common penalties
are the $L_1$ and $L_2$ penalties, defined as:

* $L_1$: $R(\wv) = \|\wv\|_1$
* $L_2$: $R(\wv) = \frac{1}{2}\|\wv\|_2^2$

The $L_2$ penalty penalizes large weights, favoring solutions with more small weights rather than
few large ones.
The $L_1$ penalty can be used to drive a number of the solution coefficients to 0, thereby
producing sparse solutions.
The optimization framework in Flink supports the $L_1$ and $L_2$ penalties, as well as no 
regularization. The 
regularization parameter $\lambda$ in $\eqref{objectiveFunc}$ determines the amount of 
regularization applied to the model,
and is usually determined through model cross-validation.

## Stochastic Gradient Descent

In order to find a (local) minimum of a function, Gradient Descent methods take steps in the
direction opposite to the gradient of the function $\eqref{objectiveFunc}$ taken with
respect to the current parameters (weights).
In order to compute the exact gradient we need to perform one pass through all the points in
a dataset, making the process computationally expensive.
An alternative is Stochastic Gradient Descent (SGD) where at each iteration we sample one point
from the complete dataset and update the parameters for each point, in an online manner.

In mini-batch SGD we instead sample random subsets of the dataset, and compute the gradient
over each batch. At each iteration of the algorithm we update the weights once, based on
the average of the gradients computed from each mini-batch.

An important parameter is the learning rate $\eta$, or step size, which is currently determined as
$\eta = \frac{\eta_0}{\sqrt{j}}$, where $\eta_0$ is the initial step size and $j$ is the iteration 
number. The setting of the initial step size can significantly affect the performance of the 
algorithm. For some practical tips on tuning SGD see Leon Botou's 
"[Stochastic Gradient Descent Tricks](http://research.microsoft.com/pubs/192769/tricks-2012.pdf)".

The current implementation of SGD  uses the whole partition, making it 
effectively a batch gradient descent. Once a sampling operator has been introduced in Flink, true
mini-batch SGD will be performed.


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
        <td><strong>Loss Function</strong></td>
        <td>
          <p>
            The class of the loss function to be used. (Default value: 
            <strong>SquaredLoss</strong>, used for regression tasks)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>RegularizationType</strong></td>
        <td>
          <p>
            The type of regularization penalty to apply. (Default value: 
            <strong>NoRegularization</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>RegularizationParameter</strong></td>
        <td>
          <p>
            The amount of regularization to apply. (Default value:<strong>0</strong>)
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
        <td><strong>Stepsize</strong></td>
        <td>
          <p>
            Initial step size for the gradient descent method.
            This value controls how far the gradient descent method moves in the opposite direction 
            of the gradient.
            (Default value: <strong>0.1</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>Prediction Function</strong></td>
        <td>
          <p>
            Class that provides the prediction function, used to calculate $\hat{y}$ based on the 
            weights and the example features, and the prediction gradient.
            (Default value: <strong>LinearPrediction</strong>)
          </p>
        </td>
      </tr>
    </tbody>
  </table>

### Examples

In the Flink implementation of SGD, given a set of examples in a `DataSet[LabeledVector]` and
optionally some initial weights, we can use `GradientDescent.optimize()` in order to optimize
the weights for the given data.

The user can provide an initial `DataSet[WeightVector]`,
which contains one `WeightVector` element, or use the default weights which are all set to 0.
A `WeightVector` is a container class for the weights, which separates the intercept from the
weight vector. This allows us to avoid applying regularization to the intercept.



{% highlight scala %}
// Create stochastic gradient descent solver
val sgd = GradientDescent()
.setLossFunction(new SquaredLoss)
.setRegularizationType(new L1Regularization)
.setRegularizationParameter(0.2)
.setIterations(100)
.setStepsize(0.01)


// Obtain data
val trainingDS: DataSet[LabeledVector] = ...

// Fit the solver to the provided data, using initial weights set to 0.0
val weightDS = sgd.optimize(inputDS, None)

// Retrieve the optimized weights
val weightVector = weightDS

// We can now use the weightVector to make predictions

{% endhighlight %}
