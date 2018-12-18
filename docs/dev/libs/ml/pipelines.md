---
mathjax: include
title: Looking under the hood of pipelines
nav-title: Pipelines
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

## Introduction

The ability to chain together different transformers and predictors is an important feature for
any Machine Learning (ML) library. In FlinkML we wanted to provide an intuitive API,
and at the same
time utilize the capabilities of the Scala language to provide
type-safe implementations of our pipelines. What we hope to achieve then is an easy to use API,
that protects users from type errors at pre-flight (before the job is launched) time, thereby
eliminating cases where long
running jobs are submitted to the cluster only to see them fail due to some
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
ML pipelines can often be complicated sets of operations ([in-depth explanation](http://research.google.com/pubs/pub43146.html)) and
can become sources of errors for end-to-end learning systems.

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

The `fit` method of the `Estimator` performs the actual training of the model, for example
finding the correct weights in a linear regression task, or the mean and standard deviation of
the data in a feature scaler.
As evident by the naming, classes that implement
`Transformer` are transform operations like [scaling the input](standard_scaler.html) and
`Predictor` implementations are learning algorithms such as [Multiple Linear Regression]({{site.baseurl}}/dev/libs/ml/multiple_linear_regression.html).
Pipelines can be created by chaining together a number of Transformers, and the final link in a pipeline can be a Predictor or another Transformer.
Pipelines that end with Predictor cannot be chained any further.
Below is an example of how a pipeline can be formed:

{% highlight scala %}
// Training data
val input: DataSet[LabeledVector] = ...
// Test data
val unlabeled: DataSet[Vector] = ...

val scaler = StandardScaler()
val polyFeatures = PolynomialFeatures()
val mlr = MultipleLinearRegression()

// Construct the pipeline
val pipeline = scaler
  .chainTransformer(polyFeatures)
  .chainPredictor(mlr)

// Train the pipeline (scaler and multiple linear regression)
pipeline.fit(input)

// Calculate predictions for the testing data
val predictions: DataSet[LabeledVector] = pipeline.predict(unlabeled)

{% endhighlight %}

As we mentioned, FlinkML pipelines are type-safe.
If we tried to chain a transformer with output of type `A` to another with input of type `B` we
would get an error at pre-flight time if `A` != `B`. FlinkML achieves this kind of type-safety
through the use of Scala's implicits.

### Scala implicits

If you are not familiar with Scala's implicits we can recommend [this excerpt](https://www.artima.com/pins1ed/implicit-conversions-and-parameters.html)
from Martin Odersky's "Programming in Scala". In short, implicit conversions allow for ad-hoc
polymorphism in Scala by providing conversions from one type to another, and implicit values
provide the compiler with default values that can be supplied to function calls through implicit parameters.
The combination of implicit conversions and implicit parameters is what allows us to chain transform
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
in the companion object of a type, so classes that implement these interfaces should provide these
objects as implicit objects inside the companion object.

As an example we can look at the `StandardScaler` class. `StandardScaler` extends `Transformer`, so it has access to its `fit` and `transform` functions.
These two functions expect objects of `FitOperation` and `TransformOperation` as implicit parameters,
for the `fit` and `transform` methods respectively, which `StandardScaler` provides in its companion
object, through `transformVectors` and `fitVectorStandardScaler`:

{% highlight scala %}
class StandardScaler extends Transformer[StandardScaler] {
  ...
}

object StandardScaler {

  ...

  implicit def fitVectorStandardScaler[T <: Vector] = new FitOperation[StandardScaler, T] {
    override def fit(instance: StandardScaler, fitParameters: ParameterMap, input: DataSet[T])
      : Unit = {
        ...
      }

  implicit def transformVectors[T <: Vector: VectorConverter: TypeInformation: ClassTag] = {
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

Note that `StandardScaler` does **not** override the `fit` method of `Estimator` or the `transform`
method of `Transformer`. Rather, its implementations of `FitOperation` and `TransformOperation`
override their respective `fit` and `transform` methods, which are then called by the `fit` and
`transform` methods of `Estimator` and `Transformer`.  Similarly, a class that implements
`Predictor` should define an implicit `PredictOperation` object inside its companion object.

#### Types and type safety

Apart from the `fit` and `transform` operations that we listed above, the `StandardScaler` also
provides `fit` and `transform` operations for input of type `LabeledVector`.
This allows us to use the  algorithm for input that is labeled or unlabeled, and this happens
automatically, depending on  the type of the input that we give to the fit and transform
operations. The correct implicit operation is chosen by the compiler, depending on the input type.

If we try to call the `fit` or `transform` methods with types that are not supported we will get a
runtime error before the job is launched.
While it would be possible to catch these kinds of errors at compile time as well, the error
messages that we are able to provide the user would be much less informative, which is why we chose
to throw runtime exceptions instead.

### Chaining

Chaining is achieved by calling `chainTransformer` or `chainPredictor` on an object
of a class that implements `Transformer`. These methods return a `ChainedTransformer` or
`ChainedPredictor` object respectively. As we mentioned, `ChainedTransformer` objects can be
chained further, while `ChainedPredictor` objects cannot. These classes take care of applying
fit, transform, and predict operations for a pair of successive transformers or
a transformer and a predictor. They also act recursively if the length of the
chain is larger than two, since every `ChainedTransformer` defines a `transform` and `fit`
operation that can be further chained with more transformers or a predictor.

It is important to note that developers and users do not need to worry about chaining when
implementing their algorithms, all this is handled automatically by FlinkML.

### How to Implement a Pipeline Operator

In order to support FlinkML's pipelining, algorithms have to adhere to a certain design pattern, which we will describe in this section.
Let's assume that we want to implement a pipeline operator which changes the mean of your data.
Since centering data is a common pre-processing step in many analysis pipelines, we will implement it as a `Transformer`.
Therefore, we first create a `MeanTransformer` class which inherits from `Transformer`

{% highlight scala %}
class MeanTransformer extends Transformer[MeanTransformer] {}
{% endhighlight %}

Since we want to be able to configure the mean of the resulting data, we have to add a configuration parameter.

{% highlight scala %}
class MeanTransformer extends Transformer[MeanTransformer] {
  def setMean(mean: Double): this.type = {
    parameters.add(MeanTransformer.Mean, mean)
    this
  }
}

object MeanTransformer {
  case object Mean extends Parameter[Double] {
    override val defaultValue: Option[Double] = Some(0.0)
  }

  def apply(): MeanTransformer = new MeanTransformer
}
{% endhighlight %}

Parameters are defined in the companion object of the transformer class and extend the `Parameter` class.
Since the parameter instances are supposed to act as immutable keys for a parameter map, they should be implemented as `case objects`.
The default value will be used if no other value has been set by the user of this component.
If no default value has been specified, meaning that `defaultValue = None`, then the algorithm has to handle this situation accordingly.

We can now instantiate a `MeanTransformer` object and set the mean value of the transformed data.
But we still have to implement how the transformation works.
The workflow can be separated into two phases.
Within the first phase, the transformer learns the mean of the given training data.
This knowledge can then be used in the second phase to transform the provided data with respect to the configured resulting mean value.

The learning of the mean can be implemented within the `fit` operation of our `Transformer`, which it inherited from `Estimator`.
Within the `fit` operation, a pipeline component is trained with respect to the given training data.
The algorithm is, however, **not** implemented by overriding the `fit` method but by providing an implementation of a corresponding `FitOperation` for the correct type.
Taking a look at the definition of the `fit` method in `Estimator`, which is the parent class of `Transformer`, reveals what why this is the case.

{% highlight scala %}
trait Estimator[Self] extends WithParameters with Serializable {
  that: Self =>

  def fit[Training](
      training: DataSet[Training],
      fitParameters: ParameterMap = ParameterMap.Empty)
      (implicit fitOperation: FitOperation[Self, Training]): Unit = {
    FlinkMLTools.registerFlinkMLTypes(training.getExecutionEnvironment)
    fitOperation.fit(this, fitParameters, training)
  }
}
{% endhighlight %}

We see that the `fit` method is called with an input data set of type `Training`, an optional parameter list and in the second parameter list with an implicit parameter of type `FitOperation`.
Within the body of the function, first some machine learning types are registered and then the `fit` method of the `FitOperation` parameter is called.
The instance gives itself, the parameter map and the training data set as a parameters to the method.
Thus, all the program logic takes place within the `FitOperation`.

The `FitOperation` has two type parameters.
The first defines the pipeline operator type for which this `FitOperation` shall work and the second type parameter defines the type of the data set elements.
If we first wanted to implement the `MeanTransformer` to work on `DenseVector`, we would, thus, have to provide an implementation for `FitOperation[MeanTransformer, DenseVector]`.

{% highlight scala %}
val denseVectorMeanFitOperation = new FitOperation[MeanTransformer, DenseVector] {
  override def fit(instance: MeanTransformer, fitParameters: ParameterMap, input: DataSet[DenseVector]) : Unit = {
    import org.apache.flink.ml.math.Breeze._
    val meanTrainingData: DataSet[DenseVector] = input
      .map{ x => (x.asBreeze, 1) }
      .reduce{
        (left, right) =>
          (left._1 + right._1, left._2 + right._2)
      }
      .map{ p => (p._1/p._2).fromBreeze }
  }
}
{% endhighlight %}

A `FitOperation[T, I]` has a `fit` method which is called with an instance of type `T`, a parameter map and an input `DataSet[I]`.
In our case `T=MeanTransformer` and `I=DenseVector`.
The parameter map is necessary if our fit step depends on some parameter values which were not given directly at creation time of the `Transformer`.
The `FitOperation` of the `MeanTransformer` sums the `DenseVector` instances of the given input data set up and divides the result by the total number of vectors.
That way, we obtain a `DataSet[DenseVector]` with a single element which is the mean value.

But if we look closely at the implementation, we see that the result of the mean computation is never stored anywhere.
If we want to use this knowledge in a later step to adjust the mean of some other input, we have to keep it around.
And here is where the parameter of type `MeanTransformer` which is given to the `fit` method comes into play.
We can use this instance to store state, which is used by a subsequent `transform` operation which works on the same object.
But first we have to extend `MeanTransformer` by a member field and then adjust the `FitOperation` implementation.

{% highlight scala %}
class MeanTransformer extends Transformer[Centering] {
  var meanOption: Option[DataSet[DenseVector]] = None

  def setMean(mean: Double): Mean = {
    parameters.add(MeanTransformer.Mean, mu)
  }
}

val denseVectorMeanFitOperation = new FitOperation[MeanTransformer, DenseVector] {
  override def fit(instance: MeanTransformer, fitParameters: ParameterMap, input: DataSet[DenseVector]) : Unit = {
    import org.apache.flink.ml.math.Breeze._

    instance.meanOption = Some(input
      .map{ x => (x.asBreeze, 1) }
      .reduce{
        (left, right) =>
          (left._1 + right._1, left._2 + right._2)
      }
      .map{ p => (p._1/p._2).fromBreeze })
  }
}
{% endhighlight %}

If we look at the `transform` method in `Transformer`, we will see that we also need an implementation of `TransformOperation`.
A possible mean transforming implementation could look like the following.

{% highlight scala %}

val denseVectorMeanTransformOperation = new TransformOperation[MeanTransformer, DenseVector, DenseVector] {
  override def transform(
      instance: MeanTransformer,
      transformParameters: ParameterMap,
      input: DataSet[DenseVector])
    : DataSet[DenseVector] = {
    val resultingParameters = parameters ++ transformParameters

    val resultingMean = resultingParameters(MeanTransformer.Mean)

    instance.meanOption match {
      case Some(trainingMean) => {
        input.map{ new MeanTransformMapper(resultingMean) }.withBroadcastSet(trainingMean, "trainingMean")
      }
      case None => throw new RuntimeException("MeanTransformer has not been fitted to data.")
    }
  }
}

class MeanTransformMapper(resultingMean: Double) extends RichMapFunction[DenseVector, DenseVector] {
  var trainingMean: DenseVector = null

  override def open(parameters: Configuration): Unit = {
    trainingMean = getRuntimeContext().getBroadcastVariable[DenseVector]("trainingMean").get(0)
  }

  override def map(vector: DenseVector): DenseVector = {
    import org.apache.flink.ml.math.Breeze._

    val result = vector.asBreeze - trainingMean.asBreeze + resultingMean

    result.fromBreeze
  }
}
{% endhighlight %}

Now we have everything implemented to fit our `MeanTransformer` to a training data set of `DenseVector` instances and to transform them.
However, when we execute the `fit` operation

{% highlight scala %}
val trainingData: DataSet[DenseVector] = ...
val meanTransformer = MeanTransformer()

meanTransformer.fit(trainingData)
{% endhighlight %}

we receive the following error at runtime: `"There is no FitOperation defined for class MeanTransformer which trains on a DataSet[org.apache.flink.ml.math.DenseVector]"`.
The reason is that the Scala compiler could not find a fitting `FitOperation` value with the right type parameters for the implicit parameter of the `fit` method.
Therefore, it chose a fallback implicit value which gives you this error message at runtime.
In order to make the compiler aware of our implementation, we have to define it as an implicit value and put it in the scope of the `MeanTransformer's` companion object.

{% highlight scala %}
object MeanTransformer{
  implicit val denseVectorMeanFitOperation = new FitOperation[MeanTransformer, DenseVector] ...

  implicit val denseVectorMeanTransformOperation = new TransformOperation[MeanTransformer, DenseVector, DenseVector] ...
}
{% endhighlight %}

Now we can call `fit` and `transform` of our `MeanTransformer` with `DataSet[DenseVector]` as input.
Furthermore, we can now use this transformer as part of an analysis pipeline where we have a `DenseVector` as input and expected output.

{% highlight scala %}
val trainingData: DataSet[DenseVector] = ...

val mean = MeanTransformer.setMean(1.0)
val polyFeatures = PolynomialFeatures().setDegree(3)

val pipeline = mean.chainTransformer(polyFeatures)

pipeline.fit(trainingData)
{% endhighlight %}

It is noteworthy that there is no additional code needed to enable chaining.
The system automatically constructs the pipeline logic using the operations of the individual components.

So far everything works fine with `DenseVector`.
But what happens, if we call our transformer with `LabeledVector` instead?
{% highlight scala %}
val trainingData: DataSet[LabeledVector] = ...

val mean = MeanTransformer()

mean.fit(trainingData)
{% endhighlight %}

As before we see the following exception upon execution of the program: `"There is no FitOperation defined for class MeanTransformer which trains on a DataSet[org.apache.flink.ml.common.LabeledVector]"`.
It is noteworthy, that this exception is thrown in the pre-flight phase, which means that the job has not been submitted to the runtime system.
This has the advantage that you won't see a job which runs for a couple of days and then fails because of an incompatible pipeline component.
Type compatibility is, thus, checked at the very beginning for the complete job.

In order to make the `MeanTransformer` work on `LabeledVector` as well, we have to provide the corresponding operations.
Consequently, we have to define a `FitOperation[MeanTransformer, LabeledVector]` and `TransformOperation[MeanTransformer, LabeledVector, LabeledVector]` as implicit values in the scope of `MeanTransformer`'s companion object.

{% highlight scala %}
object MeanTransformer {
  implicit val labeledVectorFitOperation = new FitOperation[MeanTransformer, LabeledVector] ...

  implicit val labeledVectorTransformOperation = new TransformOperation[MeanTransformer, LabeledVector, LabeledVector] ...
}
{% endhighlight %}

If we wanted to implement a `Predictor` instead of a `Transformer`, then we would have to provide a `FitOperation`, too.
Moreover, a `Predictor` requires a `PredictOperation` which implements how predictions are calculated from testing data.

{% top %}
