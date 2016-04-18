---
mathjax: include
title: Neural Networks

# Sub navigation
sub-nav-group: batch
sub-nav-parent: flinkml
sub-nav-title: Neural Networks
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

 Neural networks use a directed graph consisting of activation functions (nodes) and parameter
 weights (edges) to solve complex problems with interdependent variables.

One of the simplest types of neural networks (sometimes referred to as Artificial Neural
Networks or *ANN*s) is the multiple-layer perceptron (or *MLP*).  The Multi-Layer Perceptron is a feed forward neural network that has multiple layers which are fully connected with
*non-linear* activation function at each node and weights at each layer.

### Network Architecture

A neural network consists
- *N* input features
 - *H* hidden layers, where each layer has
   - *L<sub>k</sub>* nodes in the *l<sup>th</sup>* layer
- *M* output targets
- *L* total layers. If we consider the input features and output targets to be layers as well then the
network has *H*+2 layers in total.

Consider a simple multiple linear regression (with no intercept):

$$y = \beta_0 \cdot x_0 + \beta_1 \cdot x_1 + \beta_2 \cdot x_2 + \beta_3
\cdot x_3 + \beta_4 \cdot x_4$$

As a network graph, this linear regression would look as follows:

<center><img alt="Multiple linear regression as a graph." src="{{site.baseurl}}/apis/batch/fig/linear_regression_graph.png" width="40%"></center>

A multiple linear regression maps inputs to some output estimate at $$y$$ via a linear function.

A neural network uses hidden layer(s) of activation functions to capture interactions
between variables and mimics the biological way in which inputs are processed.

Consider a neural networks based on the same number of inputs but with two hidden layers
of five neurons each. Graphically, this network would appear as such:

<center><img alt="Multiple linear regression as a graph." src="{{site.baseurl}}/apis/batch/fig/neural_net_graph.png" width="40%"></center>

Where *h<sub>l,n</sub>* is the activation function at the *n<sup>th</sup>* node of the *l<sup>th</sup>* layer. (More on activation functions shortly.)

Flink infers the number of inputs and outputs when fitting, however the user must specify
the architecture of the hidden layers.

{% highlight scala %}
val mlp =  MultiLayerPerceptron()

val hiddenArch = List( 5, 5)

mlp.setHiddenLayerArchitecture(arch)
{% endhighlight %}

### Activation functions

Activations functions are non linear functions that (traditionally) "squash" the output to some number between zero and one.  This is akin to the biological concept of a neuron being 'active' or not.  There are three popular options available for activation functions included in Flink.

Each node/activatoin function takes as input the sum of the output of each activation in the layer below scaled by some weight. Consider the activations $$a$$ of the 0<sup>th</sup> layer are the observed features.  Each edge of the graph represents some weight $$w$$ (just as the
  $$\beta$$s represents a weight in the linear regression graph). The input of the *n*<sup>th</sup>
  node in the *l*<sup>th</sup> hidden layer is given as follows:
  <center>$$\textbf{z}_{n,l} = \sum_{i}^{N_{l-1}} w_i \cdot a_i$$</center>

Where $$N_{l-1}$$ is the number of activations in the layer below.

<table class="table table-bordered">
 <thead>
   <tr>
     <th class="text-left" style="width: 20%">Activation Function</th>
     <th class="text-left">Description</th>
     <th class="text-center">Formulation</th>
   </tr>
 </thead>
 <tbody>
 <tr>
   <td><strong>sigmoidActivationFn</strong></td>
   <td>
     <p>
       The Sigmoidal Function. The most commonly used activation function in academic literature. <a href="https://en.wikipedia.org/wiki/Sigmoid_function">see wikipedia</a>
     </p>
   </td>
   <td>$$\frac{1}{1 + e^{-\textbf{z}}}$$ </td>
 </tr>
 <tr>
   <td><strong>tanhActivationFn</strong></td>
    <td>
     <p>
      The Hyperbolic Tangent Function. Another commonly used activation function.
      <a href="http://mathworld.wolfram.com/HyperbolicTangent.html">see Wolfram</a>
     </p>
    </td>
    <td>$$\frac{e^{2\textbf{z}}-1}{e^{2\textbf{z}}+1}$$</td>
   </tr>
  <tr>
   <td><strong>elliotsSquashActivationFn</strong></td>
    <td>
     <p>A squash function proposed by David L. Elliot. This function has the desired
     properties of 'squashing' an input vector to (-1,1) and being differentiable,
     however it is simpler to compute than the Sigmoid or Hyperbolic Tangent functions.
     <a href="http://ufnalski.edu.pl/zne/ci_2014/papers/Elliott_TR_93-8.pdf">original paper</a>
     </p>
   </td>
   <td>$$\frac{\textbf{z}}{1+|\textbf{z}|}$$</td>
  </tr>
 </tbody>
</table>

By default `elliotsSquashActivationFn` is used. To change this use the
`setActivatoinFunction(...)` method.

{% highlight scala %}
val mlp =  MultiLayerPerceptron()

mlp.setActivatoinFunction(tanhActivationFn)
{% endhighlight %}

### Explicitly setting the optimizer

The Flink MultiLayer Perceptron expects the user to build an optimizer externally
and set the optimizer as an argument.  This differs because MultiLayer perceptrons can be
very complex and the user may wish to specify some other optimization strategy.

For more information on Flink-ML optimizers see [Optimization](optimization.html)

The optimizer is set with the `setOptimizer(...)` method.

{% highlight scala %}
val mlp =  MultiLayerPerceptron()

val sgd = SimpleGradientDescent()
                        .setIterations(10)
                        .setStepsize(0.5)
                        .setLearningRateMethod(LearningRateMethod.Xu(-0.75))
                        .setWarmStart(true)


mlp.setOptimizer(sgd)

{% endhighlight %}

### Full Example

This example creates two `MultiLayerPerceptron`s and optimizes them in 10 iteration
bursts to show differences in convergence with different step size strategies
(`default` vs. `Xu`).

{% highlight scala %}

// LabeledVector is a feature vector with a label (class or real value)
val trainingData: DataSet[LabeledVector] = ...
val testingData: DataSet[Vector] = ...

val mlp_default =  MultiLayerPerceptron()
                        .setOptimizer( SimpleGradientDescent()
                                .setIterations(10)
                                .setStepsize(0.5)
                                .setWarmStart(true))
                        .setHiddenLayerArchitecture(arch)

val mlp_xu =  MultiLayerPerceptron()
                        .setOptimizer( SimpleGradientDescent()
                                .setIterations(10)
                                .setStepsize(0.5)
                                .setLearningRateMethod(LearningRateMethod.Xu(-0.75))
                                .setWarmStart(true))
                        .setHiddenLayerArchitecture(arch)

println("Iteration\tDefaultSSR\tXuSSR\n")
for (i <- 1 to 40){
    mlp_default.fit(trainingData)
    mlp_xu.fit(trainingData)
    val resid_default = mlp_default.squaredResidualSum(trainingData).collect()(0)
    val resid_xu = mlp_xu.squaredResidualSum(trainingData).collect()(0)
    println(s"${(10*i).toString}\t${resid_default.toString}\t${resid_xu.toString}")
}

{% endhighlight %}
