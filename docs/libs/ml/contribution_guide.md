---
mathjax: include
htmlTitle: FlinkML - How to Contribute 
title: <a href="/libs/ml">FlinkML</a> - How to Contribute
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

The Flink community highly appreciates all sorts of contributions to FlinkML.
FlinkML offers people interested in machine learning to work on a highly active open source project which makes scalable ML reality.
The following document describes how to contribute to FlinkML.

* This will be replaced by the TOC
{:toc}

## Getting Started

In order to get started first read Flink's [contribution guide](http://flink.apache.org/how-to-contribute.html).
Everything from this guide also applies to FlinkML.

## Pick a Topic

If you are looking for some new ideas, then you should check out the list of [unresolved issues on JIRA](https://issues.apache.org/jira/issues/?jql=component%20%3D%20%22Machine%20Learning%20Library%22%20AND%20project%20%3D%20FLINK%20AND%20resolution%20%3D%20Unresolved%20ORDER%20BY%20priority%20DESC).
Once you decide to contribute to one of these issues, you should take ownership of it and track your progress with this issue.
That way, the other contributors know the state of the different issues and redundant work is avoided.

If you already know what you want to contribute to FlinkML all the better.
It is still advisable to create a JIRA issue for your idea to tell the Flink community what you want to do, though.

## Testing

New contributions should come with tests to verify the correct behavior of the algorithm.
The tests help to maintain the algorithm's correctness throughout code changes, e.g. refactorings.

We distinguish between unit tests, which are executed during Maven's test phase, and integration tests, which are executed during maven's verify phase.
Maven automatically makes this distinction by using the following naming rules:
All test cases whose class name ends with a suffix fulfilling the regular expression `(IT|Integration)(Test|Suite|Case)`, are considered integration tests.
The rest are considered unit tests and should only test behavior which is local to the component under test.

An integration test is a test which requires the full Flink system to be started.
In order to do that properly, all integration test cases have to mix in the trait `FlinkTestBase`.
This trait will set the right `ExecutionEnvironment` so that the test will be executed on a special `FlinkMiniCluster` designated for testing purposes.
Thus, an integration test could look the following:

{% highlight scala %}
class ExampleITSuite extends FlatSpec with FlinkTestBase {
  behavior of "An example algorithm"
  
  it should "do something" in {
    ...
  }
}
{% endhighlight %}

The test style does not have to be `FlatSpec` but can be any other scalatest `Suite` subclass.
See [ScalaTest testing styles](http://scalatest.org/user_guide/selecting_a_style) for more information.

## Documentation

When contributing new algorithms, it is required to add code comments describing the way the algorithm works and its parameters with which the user can control its behavior.
Additionally, we would like to encourage contributors to add this information to the online documentation.
The online documentation for FlinkML's components can be found in the directory `docs/libs/ml`.

Every new algorithm is described by a single markdown file.
This file should contain at least the following points:

1. What does the algorithm do
2. How does the algorithm work (or reference to description) 
3. Parameter description with default values
4. Code snippet showing how the algorithm is used

In order to use latex syntax in the markdown file, you have to include `mathjax: include` in the YAML front matter.
 
{% highlight java %}
---
mathjax: include
htmlTitle: FlinkML - Example title
title: <a href="/libs/ml">FlinkML</a> - Example title
---
{% endhighlight %}

In order to use displayed mathematics, you have to put your latex code in `$$ ... $$`.
For in-line mathematics, use `$ ... $`.
Additionally some predefined latex commands are included into the scope of your markdown file.
See `docs/_include/latex_commands.html` for the complete list of predefined latex commands.

## Contributing

Once you have implemented the algorithm with adequate test coverage and added documentation, you are ready to open a pull request.
Details of how to open a pull request can be found [here](http://flink.apache.org/how-to-contribute.html#contributing-code--documentation). 

## How to Implement a Pipeline Operator

FlinkML follows the principle to make machine learning as easy and accessible as possible.
Therefore, it supports a flexible pipelining mechanism which allows users to quickly define their analysis pipelines consisting of a multitude of different components.
The base trait of all pipeline operators is the `Estimator`.
The `Estimator` defines a `fit` method which is used to fit the operator to training data.
Every operator which learns from data has to implement this method.
Two sub-classes of `Estimator` are the `Transformer` and the `Predictor`.
A pipeline operator is an implementation of one of these sub-classes.

A `Transformer` defines a `transform` method which takes input data and transforms it into output data.
`Transformer`s can be stateful or stateless depending on whether they learn from training data or not.
A scaling transformer which changes the mean and variance of its input data according to the mean and variance of some training data is an example of a stateful `Transformer`.
In contrast, a transformer which maps feature vectors into the polynomial space is an example of a stateless `Transformer`.
In general, `Transformer` can be thought of as constituting the pre-processing steps in your data pipeline.

A `Predictor` defines a `predict` method which takes testing data and calculates predictions for this data.
In order to do this calculation, a `Predictor` first has to fit a model to training data.
This happens in the `fit` method which it inherits from `Estimator`.
The trained model is then used in the `predict` method to make the predictions for unseen data.
Thus, a `Predictor` is defined by the trinity of model, training logic and prediction logic.
A support vector machine, which is first trained to obtain the support vectors and then used to classify data points, is an example of a `Predictor`.

An arbitrary number of `Transformer`s with an optionally trailing `Predictor` can be chained together to form a pipeline if the input and output types are compatible.
Each operator of such a pipeline constitutes a certain task of your analysis, e.g. data centering, feature selection or model training.
Therefore, the pipeline abstraction gives you a convenient abstraction to solve complex analysis tasks.
An in-depth description of FlinkML's pipelining mechanism can be found [here]({{site.baseurl}}/libs/ml/pipelines.html).
In order to support FlinkML's pipelining, algorithms have to adhere to a certain design pattern, which we will describe next.

Let's assume that we want to implement a pipeline operator which changes the mean of your data.
Since centering data is a common pre-processing step in many analysis pipeline, we will implement it as a `Transformer`.
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
val polyFeaturs = PolynomialFeatures().setDegree(3)

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







