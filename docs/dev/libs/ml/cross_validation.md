---
mathjax: include
title: Cross Validation
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

 A prevalent problem when utilizing machine learning algorithms is *overfitting*, or when an algorithm "memorizes" the training data but does a poor job extrapolating to out of sample cases. A common method for dealing with the overfitting problem is to hold back some subset of data from the original training algorithm and then measure the fit algorithm's performance on this hold-out set. This is commonly known as *cross validation*.  A model is trained on one subset of data and then *validated* on another set of data.

## Cross Validation Strategies

There are several strategies for holding out data. FlinkML has convenience methods for
- Train-Test Splits
- Train-Test-Holdout Splits
- K-Fold Splits
- Multi-Random Splits

### Train-Test Splits

The simplest method of splitting is the `trainTestSplit`. This split takes a DataSet and a parameter *fraction*.  The *fraction* indicates the portion of the DataSet that should be allocated to the training set. This split also takes two additional optional parameters, *precise* and *seed*.  

By default, the Split is done by randomly deciding whether or not an observation is assigned to the training DataSet with probability = *fraction*.  When *precise* is `true` however, additional steps are taken to ensure the training set is as close as possible to the length of the DataSet  $\cdot$ *fraction*.

The method returns a new `TrainTestDataSet` object which has a `.training` attribute containing the training DataSet and a `.testing` attribute containing the testing DataSet.


### Train-Test-Holdout Splits

In some cases, algorithms have been known to 'learn' the testing set.  To combat this issue, a train-test-hold out strategy introduces a secondary holdout set, aptly called the *holdout* set.

Traditionally, training and testing would be done to train an algorithms as normal and then a final test of the algorithm on the holdout set would be done.  Ideally, prediction errors/model scores in the holdout set would not be significantly different than those observed in the testing set.

In a train-test-holdout strategy we sacrifice the sample size of the initial fitting algorithm for increased confidence that our model is not over-fit.

When using `trainTestHoldout` splitter, the *fraction* `Double` is replaced by a *fraction* array of length three. The first element corresponds to the portion to be used for training, second for testing, and third for holdout.  The weights of this array are *relative*, e.g. an array `Array(3.0, 2.0, 1.0)` would results in approximately 50% of the observations being in the training set, 33% of the observations in the testing set, and 17% of the observations in holdout set.

### K-Fold Splits

In a *k-fold* strategy, the DataSet is split into *k* equal subsets. Then for each of the *k* subsets, a `TrainTestDataSet` is created where the subset is the `.training` DataSet, and the remaining subsets are the `.testing` set.

For each training set, an algorithm is trained and then is evaluated based on the predictions based on the associated testing set. When an algorithm that has consistent grades (e.g. prediction errors) across held out datasets we can have some confidence that our approach (e.g. choice of algorithm / algorithm parameters / number of iterations) is robust against overfitting.

<a href="https://en.wikipedia.org/wiki/Cross-validation_(statistics)#k-fold_cross-validation">K-Fold Cross Validation</a>

### Multi-Random Splits

The *multi-random* strategy can be thought of as a more general form of the *train-test-holdout* strategy. In fact, `.trainTestHoldoutSplit` is a simple wrapper for `multiRandomSplit` which also packages the datasets into a `trainTestHoldoutDataSet` object.

The first major difference, is that `multiRandomSplit` takes an array of fractions of any length. E.g. one can create multiple holdout sets.  Alternatively, one could think of `kFoldSplit` as a wrapper for `multiRandomSplit` (which it is), the difference being `kFoldSplit` creates subsets of approximately equal size, where `multiRandomSplit` will create subsets of any size.

The second major difference is that `multiRandomSplit` returns an array of DataSets, equal in size and proportion to the *fraction array* that it was passed as an argument.

## Parameters

The various `Splitter` methods share many parameters.

 <table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Parameter</th>
      <th class="text-center">Type</th>
      <th class="text-center">Description</th>
      <th class="text-right">Used by Method</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>input</code></td>
      <td><code>DataSet[Any]</code></td>
      <td>DataSet to be split.</td>
      <td>
      <code>randomSplit</code><br>
      <code>multiRandomSplit</code><br>
      <code>kFoldSplit</code><br>
      <code>trainTestSplit</code><br>
      <code>trainTestHoldoutSplit</code>
      </td>
    </tr>
    <tr>
      <td><code>seed</code></td>
      <td><code>Long</code></td>
      <td>
        <p>
          Used for seeding the random number generator which sorts DataSets into other DataSets.
        </p>
      </td>
      <td>
      <code>randomSplit</code><br>
      <code>multiRandomSplit</code><br>
      <code>kFoldSplit</code><br>
      <code>trainTestSplit</code><br>
      <code>trainTestHoldoutSplit</code>
      </td>
    </tr>
    <tr>
      <td><code>precise</code></td>
      <td><code>Boolean</code></td>
      <td>When true, make additional effort to make DataSets as close to the prescribed proportions as possible.</td>
      <td>
      <code>randomSplit</code><br>
      <code>trainTestSplit</code>
      </td>
    </tr>
    <tr>
      <td><code>fraction</code></td>
      <td><code>Double</code></td>
      <td>The portion of the `input` to assign to the first or <code>.training</code> DataSet. Must be in the range (0,1)</td>
      <td><code>randomSplit</code><br>
        <code>trainTestSplit</code>
      </td>
    </tr>
    <tr>
      <td><code>fracArray</code></td>
      <td><code>Array[Double]</code></td>
      <td>An array that prescribes the proportions of the output datasets (proportions need not sum to 1 or be within the range (0,1))</td>
      <td>
      <code>multiRandomSplit</code><br>
      <code>trainTestHoldoutSplit</code>
      </td>
    </tr>
    <tr>
      <td><code>kFolds</code></td>
      <td><code>Int</code></td>
      <td>The number of subsets to break the <code>input</code> DataSet into.</td>
      <td><code>kFoldSplit</code></td>
      </tr>

  </tbody>
</table>

## Examples

{% highlight scala %}
// An input dataset- does not have to be of type LabeledVector
val data: DataSet[LabeledVector] = ...

// A Simple Train-Test-Split
val dataTrainTest: TrainTestDataSet = Splitter.trainTestSplit(data, 0.6, true)

// Create a train test holdout DataSet
val dataTrainTestHO: trainTestHoldoutDataSet = Splitter.trainTestHoldoutSplit(data, Array(6.0, 3.0, 1.0))

// Create an Array of K TrainTestDataSets
val dataKFolded: Array[TrainTestDataSet] =  Splitter.kFoldSplit(data, 10)

// create an array of 5 datasets
val dataMultiRandom: Array[DataSet[T]] = Splitter.multiRandomSplit(data, Array(0.5, 0.1, 0.1, 0.1, 0.1))
{% endhighlight %}

{% top %}
