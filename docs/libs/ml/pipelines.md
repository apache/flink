---
mathjax: include
title: "Pipelines - Developer's Guide"
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

## Introduction

The ability to chain together different transformers and predictors is an important feature for 
any Machine Learning (ML) library. In FlinkML we wanted to provide an intuitive API,
and at the same
time utilize the capabilities of the Scala language to provide 
type-safe implementations of our pipelines. What we hope to achieve then is an easy to use API, 
that protects users from type errors at compile time, thereby eliminating cases where long 
running jobs are submitted to the cluster only to see them fail after a long time due to some 
error in the series of data transformations that commonly happen in an ML pipeline.

In this guide then we will describe the choices we made during the implementation of chainable 
transformers and predictors in FlinkML, and provide guidelines on how developers can create their 
own algorithms that make use of these capabilities.
 
## The what and the why

So what do we mean by "ML pipelines"? Pipelines in the ML context can be thought of as chains of 
operations that have some data as input, perform a number of transformations to that data, 
and 
then output the transformed data, either to be used as the input (features) of a predictor 
function, such as a learning model, or just output the transformed data themselves, to be used in 
some other task. The end learner can of course be a part of the pipeline as well. 
ML pipelines can often be complicated sets of operations [cite ML technical debt paper] and 
can become sources 
of errors for end-to-end learning systems. 

The purpose of ML pipelines is then to create a 
framework that can be used to manage the complexity introduced by these chains of operations. 
Pipelines should make it easy for developers to define chained transformations that can be 
applied to the 
training data, in order to create the end features that will be used to train a 
learning model, and then perform the same set of transformations just as easily to unlabeled 
(test) data. Pipelines should also simplify cross-validation and model selection on 
these chains of operations.

Finally, by ensuring that the consecutive links in the pipeline chain "fit together" we also 
avoid costly type errors. Since each step in a pipeline can be a computationally-heavy operation,
we want to avoid running a pipelined job, unless we are sure that all the input/output pairs in a 
pipeline "fit".

## Pipelines in FlinkML

The building blocks for pipelines in FlinkML can be found in the `ml.pipeline` package.
FlinkML follows an API inspired by [sklearn](http://scikit-learn.org) which means that we have 
`Estimator`, `Transformer` and `Predictor` interfaces. For an in-depth look at the design of the 
sklearn API the interested reader is referred to [this](http://arxiv.org/abs/1309.0238) paper.
In short, the `Estimator` is the base class from which `Transformer` and `Predictor` inherit. 
`Estimator` defines a `fit` method, and `Transformer` also defines a `transform` method and 
`Predictor` defines a `predict` method. 

As evident by the naming, classes that implement 
`Transformer` are transform operations like [scaling the input](standard_scaler.html) and 
`Predictor` implementations are learning algorithms such as [Multiple Linear Regression]
(multiple_linear_regression.html).
Pipelines can be created by chaining together a 
number of Transformers, and the final link in a pipeline can be a Predictor or another Transformer.
Pipelines that end with Predictor cannot be chained any further.
Below is an example of how a pipeline can be formed:

TODO: Make the example up-to-date

{% highlight scala %}
// Training data
val input: DataSet[LabeledVector] = ???
// Test data
val unlabeled: DataSet[Vector] = ???

val scaler = new Scaler
val offset = new Offset
val kmeans = new KMeans()

val chain = scaler
  .chainTransformer(offset)
  .chainPredictor(kmeans)
  
chain.fit(input)

val predictions = chain.predict(unlabeled)

{% endhighlight %}

As we mentioned, FlinkML pipelines are type-safe. 
If we tried to chain a transformer with output of type `A` to another with input of type `B` we 
would get an error at compile time if `A` != `B`. FlinkML achieves this kind of type-safety through
the use of 
Scala's implicits. 

### Scala implicits

If you are not familiar with Scala's implicits we can recommend [this excerpt](https://www.artima.com/pins1ed/implicit-conversions-and-parameters.html) 
from Martin Odersky's "Programming in Scala". In short, implicit conversions allow for ad-hoc 
polymorphism in Scala by providing conversions from one type to another, and implicit parameters
provide the compiler with default values that can be supplied to function calls. The 
combination of implicit conversions and implicit parameters is what allows us to chain transform 
and predict operations together in a type-safe manner.

### Operations

As we mentioned, the trait (abstract class) `Estimator` defines a `fit` method. The method has two 
parameter lists 
(i.e. is a [curried function](http://docs.scala-lang.org/tutorials/tour/currying.html)). The 
first parameter list
takes the input (training) `DataSet` and the parameters for the estimator. The second parameter 
list takes one `implicit` parameter, of type `FitOperation`. `FitOperation` is a class that also 
defines a `fit` method, and this is where the actual logic of training the concrete Estimators 
should be implemented. The `fit` method of `Estimator` is essentially a wrapper around the  fit 
method of `FitOperation`. The `predict` method of `Predictor` and the `transform` method of 
`Transform` are designed in a similar manner, with a respective operation class.

In these methods the operation object is provided as an implicit parameter.
Scala will [look for implicits](http://docs.scala-lang.org/tutorials/FAQ/finding-implicits.html) 
in the companion object of a type, so classes that
implement these interfaces should provide these objects as implicit objects inside 
the companion object. 

As an example we can look at the `StandardScaler` class. `StandardScaler` extends `Transformer`, 
so it has access to its `fit` and `transform` functions.
These two functions expect objects of `FitOperation` and `TransformOperation` as implicit 
parameters,
for the `fit` and `transform` methods respectively, which `StandardScaler` provides in its 
companion object, through `scaleVectors` and `fitMeanAndStd`:

{% highlight scala %}
class StandardScaler extends Transformer[StandardScaler] {
  ...
}
  
object StandardScaler {
  
  ...
  
  implicit def fitMeanAndStd[T <: Vector] = new FitOperation[StandardScaler, T] {
    override def fit(instance: StandardScaler, fitParameters: ParameterMap, input: DataSet[T])
      : Unit = {
      ...
      }
  
  implicit def scaleVectors[T <: Vector: VectorConverter: TypeInformation: ClassTag] = {
      new TransformOperation[StandardScaler, T, T] {
        override def transform(
          instance: StandardScaler,
          transformParameters: ParameterMap,
          input: DataSet[T])
        : DataSet[T] = {
        ...
        }
  
}

{% endhighlight %}

Note that `StandardScaler` does _not_ override the `fit` method of `Estimator` or the `transform` 
method of `Transformer`. Rather, its implementations of `FitOperation` and `TransformOperation` 
override their respective `fit` and `transform` methods, which are then called by the `fit` and
`transform` methods of `Estimator` and `Transformer`.  Similarly, a class that implements 
`Predictor` should define an implicit `PredictOperation` object inside its companion object.

### Chaining

Chaining is achieved by calling `chainTransformer` or `chainPredictor` on a an object 
of a class that implements `Transformer`. These methods return a `ChainedTransformer` or 
`ChainedPredictor` object respectively. As we mentioned, `ChainedTransformer` objects can be 
chained further, while `ChainedPredictor` objects cannot. These classes take care of applying 
fit, transform, and predict operations for a pair of successive transformers or 
a transformer and a predictor. They also act recursively if the length of the 
chain is larger than two, since every `ChainedTransformer` defines a `transform` and `fit` 
operation 
that can be further chained with more transformers or a predictor.


## Implementing new Transformer and Predictor algorithms 

Given what we saw above implementing new `Transformer` and `Predictor` algorithms is 
straightforward. In both cases the class should provide _in its companion object_ an implicit 
object of the 
`FitOperation` class, which overrides the `fit` method. Classes that 
implement `Transformer` should also have an implicit object of the `TransformOperation` class
that overrides the `transform` method, and similarly with `Predictor`, `PredictOperation` and
`predict`.

TODO: Write a few things about the generic types that should be provided for the operations
