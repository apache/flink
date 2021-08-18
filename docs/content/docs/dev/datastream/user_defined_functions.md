---
title: User-Defined Functions
weight: 5
type: docs
aliases:
  - /dev/user_defined_functions.html
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

# User-Defined Functions

Most operations require a user-defined function. This section lists different
ways of how they can be specified. We also cover `Accumulators`, which can be
used to gain insights into your Flink application.

{{< tabs "ff746fe9-c77d-443f-b7ec-41716f968349" >}}
{{< tab "Java" >}}

## Implementing an interface

The most basic way is to implement one of the provided interfaces:

```java
class MyMapFunction implements MapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
};
data.map(new MyMapFunction());
```

## Anonymous classes

You can pass a function as an anonymous class:
```java
data.map(new MapFunction<String, Integer> () {
  public Integer map(String value) { return Integer.parseInt(value); }
});
```

## Java 8 Lambdas

Flink also supports Java 8 Lambdas in the Java API.

```java
data.filter(s -> s.startsWith("http://"));
```

```java
data.reduce((i1,i2) -> i1 + i2);
```

## Rich functions

All transformations that require a user-defined function can
instead take as argument a *rich* function. For example, instead of

```java
class MyMapFunction implements MapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
};
```

you can write

```java
class MyMapFunction extends RichMapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
};
```

and pass the function as usual to a `map` transformation:

```java
data.map(new MyMapFunction());
```

Rich functions can also be defined as an anonymous class:
```java
data.map (new RichMapFunction<String, Integer>() {
  public Integer map(String value) { return Integer.parseInt(value); }
});
```

{{< /tab >}}
{{< tab "Scala" >}}

## Lambda Functions

As already seen in previous examples all operations accept lambda functions for describing
the operation:
```scala
val data: DataSet[String] = // [...]
data.filter { _.startsWith("http://") }
```

```scala
val data: DataSet[Int] = // [...]
data.reduce { (i1,i2) => i1 + i2 }
// or
data.reduce { _ + _ }
```

## Rich functions

All transformations that take as argument a lambda function can
instead take as argument a *rich* function. For example, instead of

```scala
data.map { x => x.toInt }
```

you can write

```scala
class MyMapFunction extends RichMapFunction[String, Int] {
  def map(in: String):Int = { in.toInt }
};
```

and pass the function to a `map` transformation:

```scala
data.map(new MyMapFunction())
```

Rich functions can also be defined as an anonymous class:
```scala
data.map (new RichMapFunction[String, Int] {
  def map(in: String):Int = { in.toInt }
})
```
{{< /tab >}}
{{< /tabs >}}

Rich functions provide, in addition to the user-defined function (map,
reduce, etc), four methods: `open`, `close`, `getRuntimeContext`, and
`setRuntimeContext`. These are useful for parameterizing the function
(see [Passing Parameters to Functions]({{< ref "docs/dev/dataset/overview" >}}#passing-parameters-to-functions)),
creating and finalizing local state, accessing broadcast variables (see
[Broadcast Variables]({{< ref "docs/dev/dataset/overview" >}}#broadcast-variables)), and for accessing runtime
information such as accumulators and counters (see
[Accumulators and Counters](#accumulators--counters)), and information
on iterations (see [Iterations]({{< ref "docs/dev/dataset/iterations" >}})).

{{< top >}}

## Accumulators & Counters

Accumulators are simple constructs with an **add operation** and a **final accumulated result**,
which is available after the job ended.

The most straightforward accumulator is a **counter**: You can increment it using the
```Accumulator.add(V value)``` method. At the end of the job Flink will sum up (merge) all partial
results and send the result to the client. Accumulators are useful during debugging or if you
quickly want to find out more about your data.

Flink currently has the following **built-in accumulators**. Each of them implements the
{{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/accumulators/Accumulator.java" name="Accumulator" >}}
interface.

- {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/accumulators/IntCounter.java" name="__IntCounter__" >}},
  {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/accumulators/LongCounter.java" name="__LongCounter__" >}}
  and {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/accumulators/DoubleCounter.java" name="__DoubleCounter__" >}}:
  See below for an example using a counter.
- {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/accumulators/Histogram.java" name="__Histogram__" >}}:
  A histogram implementation for a discrete number of bins. Internally it is just a map from Integer
  to Integer. You can use this to compute distributions of values, e.g. the distribution of
  words-per-line for a word count program.

__How to use accumulators:__

First you have to create an accumulator object (here a counter) in the user-defined transformation
function where you want to use it.

```java
private IntCounter numLines = new IntCounter();
```

Second you have to register the accumulator object, typically in the ```open()``` method of the
*rich* function. Here you also define the name.

```java
getRuntimeContext().addAccumulator("num-lines", this.numLines);
```

You can now use the accumulator anywhere in the operator function, including in the ```open()``` and
```close()``` methods.

```java
this.numLines.add(1);
```

The overall result will be stored in the ```JobExecutionResult``` object which is
returned from the `execute()` method of the execution environment
(currently this only works if the execution waits for the
completion of the job).

```java
myJobExecutionResult.getAccumulatorResult("num-lines")
```

All accumulators share a single namespace per job. Thus you can use the same accumulator in
different operator functions of your job. Flink will internally merge all accumulators with the same
name.

A note on accumulators and iterations: Currently the result of accumulators is only available after
the overall job has ended. We plan to also make the result of the previous iteration available in the
next iteration. You can use
{{< gh_link file="/flink-java/src/main/java/org/apache/flink/api/java/operators/IterativeDataSet.java#L98" name="Aggregators" >}}
to compute per-iteration statistics and base the termination of iterations on such statistics.

__Custom accumulators:__

To implement your own accumulator you simply have to write your implementation of the Accumulator
interface. Feel free to create a pull request if you think your custom accumulator should be shipped
with Flink.

You have the choice to implement either
{{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/accumulators/Accumulator.java" name="Accumulator" >}}
or {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/accumulators/SimpleAccumulator.java" name="SimpleAccumulator" >}}.

```Accumulator<V,R>``` is most flexible: It defines a type ```V``` for the value to add, and a
result type ```R``` for the final result. E.g. for a histogram, ```V``` is a number and ```R``` is
 a histogram. ```SimpleAccumulator``` is for the cases where both types are the same, e.g. for counters.

{{< top >}}
