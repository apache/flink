---
title: 'User Defined Functions'
nav-id: user_defined_function
nav-parent_id: streaming
nav-pos: 4
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

Most operations require a user-defined function. This section lists different
ways of how they can be specified.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

## Implementing an interface

The most basic way is to implement one of the provided interfaces:

{% highlight java %}
class MyMapFunction implements MapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
};
data.map(new MyMapFunction());
{% endhighlight %}

## Anonymous classes

You can pass a function as an anonymous class:
{% highlight java %}
data.map(new MapFunction<String, Integer> () {
  public Integer map(String value) { return Integer.parseInt(value); }
});
{% endhighlight %}

## Java 8 Lambdas

Flink also supports Java 8 Lambdas in the Java API.

{% highlight java %}
data.filter(s -> s.startsWith("http://"));
{% endhighlight %}

{% highlight java %}
data.reduce((i1,i2) -> i1 + i2);
{% endhighlight %}

## Rich functions

All transformations that require a user-defined function can
instead take as argument a *rich* function. For example, instead of

{% highlight java %}
class MyMapFunction implements MapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
};
{% endhighlight %}

you can write

{% highlight java %}
class MyMapFunction extends RichMapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
};
{% endhighlight %}

and pass the function as usual to a `map` transformation:

{% highlight java %}
data.map(new MyMapFunction());
{% endhighlight %}

Rich functions can also be defined as an anonymous class:
{% highlight java %}
data.map (new RichMapFunction<String, Integer>() {
  public Integer map(String value) { return Integer.parseInt(value); }
});
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">


## Lambda Functions

As already seen in previous examples all operations accept lambda functions for describing
the operation:
{% highlight scala %}
val data: DataSet[String] = // [...]
data.filter { _.startsWith("http://") }
{% endhighlight %}

{% highlight scala %}
val data: DataSet[Int] = // [...]
data.reduce { (i1,i2) => i1 + i2 }
// or
data.reduce { _ + _ }
{% endhighlight %}

## Rich functions

All transformations that take as argument a lambda function can
instead take as argument a *rich* function. For example, instead of

{% highlight scala %}
data.map { x => x.toInt }
{% endhighlight %}

you can write

{% highlight scala %}
class MyMapFunction extends RichMapFunction[String, Int] {
  def map(in: String):Int = { in.toInt }
};
{% endhighlight %}

and pass the function to a `map` transformation:

{% highlight scala %}
data.map(new MyMapFunction())
{% endhighlight %}

Rich functions can also be defined as an anonymous class:
{% highlight scala %}
data.map (new RichMapFunction[String, Int] {
  def map(in: String):Int = { in.toInt }
})
{% endhighlight %}
</div>

</div>

Rich functions provide, in addition to the user-defined function (map,
reduce, etc), four methods: `open`, `close`, `getRuntimeContext`, and
`setRuntimeContext`. These are useful for parameterizing the function
(see [Passing Parameters to Functions]({{ site.baseurl }}/dev/batch/index.html#passing-parameters-to-functions)),
creating and finalizing local state, accessing broadcast variables (see
[Broadcast Variables]({{ site.baseurl }}/dev/batch/index.html#broadcast-variables)), and for accessing runtime
information such as accumulators and counters (see
[Accumulators and Counters](#accumulators--counters)), and information
on iterations (see [Iterations]({{ site.baseurl }}/dev/batch/iterations.html)).

{% top %}

