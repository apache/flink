---
mathjax: include
title: Alternating Least Squares
nav-title: ALS
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

The alternating least squares (ALS) algorithm factorizes a given matrix $R$ into two factors $U$ and $V$ such that $R \approx U^TV$.
The unknown row dimension is given as a parameter to the algorithm and is called latent factors.
Since matrix factorization can be used in the context of recommendation, the matrices $U$ and $V$ can be called user and item matrix, respectively.
The $i$th column of the user matrix is denoted by $u_i$ and the $i$th column of the item matrix is $v_i$.
The matrix $R$ can be called the ratings matrix with $$(R)_{i,j} = r_{i,j}$$.

In order to find the user and item matrix, the following problem is solved:

$$\arg\min_{U,V} \sum_{\{i,j\mid r_{i,j} \not= 0\}} \left(r_{i,j} - u_{i}^Tv_{j}\right)^2 +
\lambda \left(\sum_{i} n_{u_i} \left\lVert u_i \right\rVert^2 + \sum_{j} n_{v_j} \left\lVert v_j \right\rVert^2 \right)$$

with $\lambda$ being the regularization factor, $$n_{u_i}$$ being the number of items the user $i$ has rated and $$n_{v_j}$$ being the number of times the item $j$ has been rated.
This regularization scheme to avoid overfitting is called weighted-$\lambda$-regularization.
Details can be found in the work of [Zhou et al.](http://dx.doi.org/10.1007/978-3-540-68880-8_32).

By fixing one of the matrices $U$ or $V$, we obtain a quadratic form which can be solved directly.
The solution of the modified problem is guaranteed to monotonically decrease the overall cost function.
By applying this step alternately to the matrices $U$ and $V$, we can iteratively improve the matrix factorization.

The matrix $R$ is given in its sparse representation as a tuple of $(i, j, r)$ where $i$ denotes the row index, $j$ the column index and $r$ is the matrix value at position $(i,j)$.

## Operations

`ALS` is a `Predictor`.
As such, it supports the `fit` and `predict` operation.

### Fit

ALS is trained on the sparse representation of the rating matrix:

* `fit: DataSet[(Int, Int, Double)] => Unit`

### Predict

ALS predicts for each tuple of row and column index the rating:

* `predict: DataSet[(Int, Int)] => DataSet[(Int, Int, Double)]`

## Parameters

The alternating least squares implementation can be controlled by the following parameters:

   <table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 20%">Parameters</th>
        <th class="text-center">Description</th>
      </tr>
    </thead>

    <tbody>
      <tr>
        <td><strong>NumFactors</strong></td>
        <td>
          <p>
            The number of latent factors to use for the underlying model.
            It is equivalent to the dimension of the calculated user and item vectors.
            (Default value: <strong>10</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>Lambda</strong></td>
        <td>
          <p>
            Regularization factor. Tune this value in order to avoid overfitting or poor performance due to strong generalization.
            (Default value: <strong>1</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>Iterations</strong></td>
        <td>
          <p>
            The maximum number of iterations.
            (Default value: <strong>10</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>Blocks</strong></td>
        <td>
          <p>
            The number of blocks into which the user and item matrix are grouped.
            The fewer blocks one uses, the less data is sent redundantly.
            However, bigger blocks entail bigger update messages which have to be stored on the heap.
            If the algorithm fails because of an OutOfMemoryException, then try to increase the number of blocks.
            (Default value: <strong>None</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>Seed</strong></td>
        <td>
          <p>
            Random seed used to generate the initial item matrix for the algorithm.
            (Default value: <strong>0</strong>)
          </p>
        </td>
      </tr>
      <tr>
        <td><strong>TemporaryPath</strong></td>
        <td>
          <p>
            Path to a temporary directory into which intermediate results are stored.
            If this value is set, then the algorithm is split into two preprocessing steps, the ALS iteration and a post-processing step which calculates a last ALS half-step.
            The preprocessing steps calculate the <code>OutBlockInformation</code> and <code>InBlockInformation</code> for the given rating matrix.
            The results of the individual steps are stored in the specified directory.
            By splitting the algorithm into multiple smaller steps, Flink does not have to split the available memory amongst too many operators.
            This allows the system to process bigger individual messages and improves the overall performance.
            (Default value: <strong>None</strong>)
          </p>
        </td>
      </tr>
    </tbody>
  </table>

## Examples

{% highlight scala %}
// Read input data set from a csv file
val inputDS: DataSet[(Int, Int, Double)] = env.readCsvFile[(Int, Int, Double)](
  pathToTrainingFile)

// Setup the ALS learner
val als = ALS()
.setIterations(10)
.setNumFactors(10)
.setBlocks(100)
.setTemporaryPath("hdfs://tempPath")

// Set the other parameters via a parameter map
val parameters = ParameterMap()
.add(ALS.Lambda, 0.9)
.add(ALS.Seed, 42L)

// Calculate the factorization
als.fit(inputDS, parameters)

// Read the testing data set from a csv file
val testingDS: DataSet[(Int, Int)] = env.readCsvFile[(Int, Int)](pathToData)

// Calculate the ratings according to the matrix factorization
val predictedRatings = als.predict(testingDS)
{% endhighlight %}

{% top %}
