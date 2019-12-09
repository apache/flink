---
title: "Best Practices"
nav-parent_id: dev
nav-pos: 90
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

This page contains a collection of best practices for Flink programmers on how to solve frequently encountered problems.


* This will be replaced by the TOC
{:toc}

## Parsing command line arguments and passing them around in your Flink application

Almost all Flink applications, both batch and streaming, rely on external configuration parameters.
They are used to specify input and output sources (like paths or addresses), system parameters (parallelism, runtime configuration), and application specific parameters (typically used within user functions).

Flink provides a simple utility called `ParameterTool` to provide some basic tooling for solving these problems.
Please note that you don't have to use the `ParameterTool` described here. Other frameworks such as [Commons CLI](https://commons.apache.org/proper/commons-cli/) and
[argparse4j](http://argparse4j.sourceforge.net/) also work well with Flink.


### Getting your configuration values into the `ParameterTool`

The `ParameterTool` provides a set of predefined static methods for reading the configuration. The tool is internally expecting a `Map<String, String>`, so it's very easy to integrate it with your own configuration style.


#### From `.properties` files

The following method will read a [Properties](https://docs.oracle.com/javase/tutorial/essential/environment/properties.html) file and provide the key/value pairs:
{% highlight java %}
String propertiesFilePath = "/home/sam/flink/myjob.properties";
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);

File propertiesFile = new File(propertiesFilePath);
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);

InputStream propertiesFileInputStream = new FileInputStream(file);
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFileInputStream);
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

You can use the return values of these methods directly in the `main()` method of the client submitting the application.
For example, you could set the parallelism of a operator like this:

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

and then use it inside the function for getting values from the command line.

#### Register the parameters globally

Parameters registered as global job parameters in the `ExecutionConfig` can be accessed as configuration values from the JobManager web interface and in all functions defined by the user.

Register the parameters globally:

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
	ParameterTool parameters = (ParameterTool)
	    getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
	parameters.getRequired("input");
	// .. do more ..
{% endhighlight %}


## Naming large TupleX types

It is recommended to use POJOs (Plain old Java objects) instead of `TupleX` for data types with many fields.
Also, POJOs can be used to give large `Tuple`-types a name.

**Example**

Instead of using:


{% highlight java %}
Tuple11<String, String, ..., String> var = new ...;
{% endhighlight %}


It is much easier to create a custom type extending from the large Tuple type.

{% highlight java %}
CustomType var = new ...;

public static class CustomType extends Tuple11<String, String, ..., String> {
    // constructor matching super
}
{% endhighlight %}

## Using Logback instead of Log4j

**Note: This tutorial is applicable starting from Flink 0.10**

Apache Flink is using [slf4j](http://www.slf4j.org/) as the logging abstraction in the code. Users are advised to use sfl4j as well in their user functions.

Sfl4j is a compile-time logging interface that can use different logging implementations at runtime, such as [log4j](http://logging.apache.org/log4j/2.x/) or [Logback](http://logback.qos.ch/).

Flink is depending on Log4j by default. This page describes how to use Flink with Logback. Users reported that they were also able to set up centralized logging with Graylog using this tutorial.

To get a logger instance in the code, use the following code:


{% highlight java %}
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyClass implements MapFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MyClass.class);
    // ...
{% endhighlight %}


### Use Logback when running Flink out of the IDE / from a Java application


In all cases where classes are executed with a classpath created by a dependency manager such as Maven, Flink will pull log4j into the classpath.

Therefore, you will need to exclude log4j from Flink's dependencies. The following description will assume a Maven project created from a [Flink quickstart](./projectsetup/java_api_quickstart.html).

Change your projects `pom.xml` file like this:

{% highlight xml %}
<dependencies>
	<!-- Add the two required logback dependencies -->
	<dependency>
		<groupId>ch.qos.logback</groupId>
		<artifactId>logback-core</artifactId>
		<version>1.1.3</version>
	</dependency>
	<dependency>
		<groupId>ch.qos.logback</groupId>
		<artifactId>logback-classic</artifactId>
		<version>1.1.3</version>
	</dependency>

	<!-- Add the log4j -> sfl4j (-> logback) bridge into the classpath
	 Hadoop is logging to log4j! -->
	<dependency>
		<groupId>org.slf4j</groupId>
		<artifactId>log4j-over-slf4j</artifactId>
		<version>1.7.7</version>
	</dependency>

	<dependency>
		<groupId>org.apache.flink</groupId>
		<artifactId>flink-java</artifactId>
		<version>{{ site.version }}</version>
		<exclusions>
			<exclusion>
				<groupId>log4j</groupId>
				<artifactId>*</artifactId>
			</exclusion>
			<exclusion>
				<groupId>org.slf4j</groupId>
				<artifactId>slf4j-log4j12</artifactId>
			</exclusion>
		</exclusions>
	</dependency>
	<dependency>
		<groupId>org.apache.flink</groupId>
		<artifactId>flink-streaming-java{{ site.scala_version_suffix }}</artifactId>
		<version>{{ site.version }}</version>
		<exclusions>
			<exclusion>
				<groupId>log4j</groupId>
				<artifactId>*</artifactId>
			</exclusion>
			<exclusion>
				<groupId>org.slf4j</groupId>
				<artifactId>slf4j-log4j12</artifactId>
			</exclusion>
		</exclusions>
	</dependency>
	<dependency>
		<groupId>org.apache.flink</groupId>
		<artifactId>flink-clients{{ site.scala_version_suffix }}</artifactId>
		<version>{{ site.version }}</version>
		<exclusions>
			<exclusion>
				<groupId>log4j</groupId>
				<artifactId>*</artifactId>
			</exclusion>
			<exclusion>
				<groupId>org.slf4j</groupId>
				<artifactId>slf4j-log4j12</artifactId>
			</exclusion>
		</exclusions>
	</dependency>
</dependencies>
{% endhighlight %}

The following changes were done in the `<dependencies>` section:

 * Exclude all `log4j` dependencies from all Flink dependencies: this causes Maven to ignore Flink's transitive dependencies to log4j.
 * Exclude the `slf4j-log4j12` artifact from Flink's dependencies: since we are going to use the slf4j to logback binding, we have to remove the slf4j to log4j binding.
 * Add the Logback dependencies: `logback-core` and `logback-classic`
 * Add dependencies for `log4j-over-slf4j`. `log4j-over-slf4j` is a tool which allows legacy applications which are directly using the Log4j APIs to use the Slf4j interface. Flink depends on Hadoop which is directly using Log4j for logging. Therefore, we need to redirect all logger calls from Log4j to Slf4j which is in turn logging to Logback.

Please note that you need to manually add the exclusions to all new Flink dependencies you are adding to the pom file.

You may also need to check if other (non-Flink) dependencies are pulling in log4j bindings. You can analyze the dependencies of your project with `mvn dependency:tree`.



### Use Logback when running Flink on a cluster

This tutorial is applicable when running Flink on YARN or as a standalone cluster.

In order to use Logback instead of Log4j with Flink, you need to remove `log4j-1.2.xx.jar` and `sfl4j-log4j12-xxx.jar` from the `lib/` directory.

Next, you need to put the following jar files into the `lib/` folder:

 * `logback-classic.jar`
 * `logback-core.jar`
 * `log4j-over-slf4j.jar`: This bridge needs to be present in the classpath for redirecting logging calls from Hadoop (which is using Log4j) to Slf4j.

Note that you need to explicitly set the `lib/` directory when using a per-job YARN cluster.

The command to submit Flink on YARN with a custom logger is: `./bin/flink run -yt $FLINK_HOME/lib <... remaining arguments ...>`

{% top %}
