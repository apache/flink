---
title: "Best Practices"
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

<a href="#top"></a>


This page contains a collection of best practices for Flink programmers on how to solve frequently encountered problems.


* This will be replaced by the TOC
{:toc}

## Parsing command line arguments and passing them around in your Flink application


Almost all Flink applications, both batch and streaming rely on external configuration parameters.
For example for specifying input and output sources (like paths or addresses), also system parameters (parallelism, runtime configuration) and application specific parameters (often used within the user functions).

Since version 0.9 we are providing a simple utility called `ParameterTool` to provide at least some basic tooling for solving these problems.

Please note that you don't have to use the `ParameterTool` explained here. Other frameworks such as [Commons CLI](https://commons.apache.org/proper/commons-cli/), 
[argparse4j](http://argparse4j.sourceforge.net/) and others work well with Flink as well.


### Getting your configuration values into the `ParameterTool`

The `ParameterTool` provides a set of predefined static methods for reading the configuration. The tool is internally expecting a `Map<String, String>`, so its very easy to integrate it with your own configuration style.


#### From `.properties` files

The following method will read a [Properties](https://docs.oracle.com/javase/tutorial/essential/environment/properties.html) file and provide the key/value pairs:
{% highlight java %}
String propertiesFile = "/home/sam/flink/myjob.properties";
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);
{% endhighlight %}


#### From the command line arguments

This allows getting arguments like `--input hdfs:///mydata --elements 42` from the command line.
{% highlight java %}
public static void main(String[] args) {
	ParameterTool parameter = ParameterTool.fromArgs(args);
	// .. regular code ..
{% endhighlight %}


#### From system properties

When starting a JVM, you can pass system properties to it: `-Dinput=hdfs:///mydata`. You can also initialize the `ParameterTool` from these system properties:

{% highlight java %}
ParameterTool parameter = ParameterTool.fromSystemProperties();
{% endhighlight %}


### Using the parameters in your Flink program

Now that we've got the parameters from somewhere (see above) we can use them in various ways.

**Directly from the `ParameterTool`**

The `ParameterTool` itself has methods for accessing the values.
{% highlight java %}
ParameterTool parameters = // ...
parameter.getRequired("input");
parameter.get("output", "myDefaultValue");
parameter.getLong("expectedCount", -1L);
parameter.getNumberOfParameters()
// .. there are more methods available.
{% endhighlight %}

You can use the return values of these methods directly in the main() method (=the client submitting the application).
For example you could set the parallelism of a operator like this:

{% highlight java %}
ParameterTool parameters = ParameterTool.fromArgs(args);
int parallelism = parameters.get("mapParallelism", 2);
DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).setParallelism(parallelism);
{% endhighlight %}

Since the `ParameterTool` is serializable, you can pass it to the functions itself:

{% highlight java %}
ParameterTool parameters = ParameterTool.fromArgs(args);
DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer(parameters));
{% endhighlight %}

and then use them inside the function for getting values from the command line.


#### Passing it as a `Configuration` object to single functions

The example below shows how to pass the parameters as a `Configuration` object to a user defined function.

{% highlight java %}
ParameterTool parameters = ParameterTool.fromArgs(args);
DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).withParameters(parameters.getConfiguration())
{% endhighlight %}

In the `Tokenizer`, the object is now accessible in the `open(Configuration conf)` method:

{% highlight java %}
public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
	@Override
	public void open(Configuration parameters) throws Exception {
		parameters.getInteger("myInt", -1);
		// .. do 
{% endhighlight %}


#### Register the parameters globally

Parameters registered as a [global job parameter](programming_guide.html#passing-parameters-to-functions) at the `ExecutionConfig` allow you to access the configuration values from the JobManager web interface and all functions defined by the user.

**Register the parameters globally**

{% highlight java %}
ParameterTool parameters = ParameterTool.fromArgs(args);

// set up the execution environment
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().setGlobalJobParameters(parameters);
{% endhighlight %}

Access them in any rich user function:

{% highlight java %}
public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

	@Override
	public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
		ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		parameters.getRequired("input");
		// .. do more ..
{% endhighlight %}

