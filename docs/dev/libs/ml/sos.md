---
mathjax: include
title: Stochastic Outlier Selection
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

An outlier is one or multiple observations that deviates quantitatively from the majority of the data set and may be the subject of further investigation.
Stochastic Outlier Selection (SOS) developed by Jeroen Janssens[[1]](#janssens) is an unsupervised outlier-selection algorithm that takes as input a set of 
vectors. The algorithm applies affinity-based outlier selection and outputs for each data point an outlier probability. 
Intuitively, a data point is considered to be an outlier when the other data points have insufficient affinity with it.

Outlier detection has its application in a number of field, for example, log analysis, fraud detection, noise removal, novelty detection, quality control,
 sensor monitoring, etc. If a sensor turns faulty, it is likely that it will output values that deviate markedly from the majority.
 
For more information, please consult the [PhD Thesis of Jeroens Janssens](https://github.com/jeroenjanssens/phd-thesis) on 
Outlier Selection and One-Class Classification which introduces the algorithm.                                                                        

## Parameters

The stochastic outlier selection algorithm implementation can be controlled by the following parameters:

   <table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 20%">Parameters</th>
        <th class="text-center">Description</th>
      </tr>
    </thead>

    <tbody>
      <tr>
        <td><strong>Perplexity</strong></td>
        <td>
          <p>
            Perplexity can be interpreted as the k in k-nearest neighbor algorithms. The difference with SOS being a neighbor
            is not a binary property, but a probabilistic one, and therefore it a real number. Must be between 1 and n-1, 
            where n is the number of points. A good starting point can be obtained by using the square root of the number of observations. 
            (Default value: <strong>30</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>ErrorTolerance</strong></td>
        <td>
          <p>
            The accepted error tolerance to reduce computational time when approximating the affinity. It will 
            sacrifice accuracy in return for reduced computational time.
            (Default value: <strong>1e-20</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>MaxIterations</strong></td>
        <td>
          <p>
            The maximum number of iterations to approximate the affinity of the algorithm.
            (Default value: <strong>10</strong>)
          </p>
        </td>
      </tr>
    </tbody>
  </table>


## Example

{% highlight scala %}
val data = env.fromCollection(List(
  LabeledVector(0.0, DenseVector(1.0, 1.0)),
  LabeledVector(1.0, DenseVector(2.0, 1.0)),
  LabeledVector(2.0, DenseVector(1.0, 2.0)),
  LabeledVector(3.0, DenseVector(2.0, 2.0)),
  LabeledVector(4.0, DenseVector(5.0, 8.0)) // The outlier!
))

val sos = new StochasticOutlierSelection().setPerplexity(3)

val outputVector = sos
  .transform(data)
  .collect()

val expectedOutputVector = Map(
  0 -> 0.2790094479202896,
  1 -> 0.25775014551682535,
  2 -> 0.22136130977995766,
  3 -> 0.12707053787018444,
  4 -> 0.9922779902453757 // The outlier!
)

outputVector.foreach(output => expectedOutputVector(output._1) should be(output._2))
{% endhighlight %}

**References**

<a name="janssens"></a>[1]J.H.M. Janssens, F. Huszar, E.O. Postma, and H.J. van den Herik. 
*Stochastic Outlier Selection*. Technical Report TiCC TR 2012-001, Tilburg University, Tilburg, the Netherlands, 2012.

{% top %}
