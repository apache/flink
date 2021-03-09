---
title: "Handling Application Parameters"
weight: 51
type: docs
aliases:
  - /zh/dev/application_parameters.html
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

# Handling Application Parameters



Handling Application Parameters
-------------------------------
Almost all Flink applications, both batch and streaming, rely on external configuration parameters.
They are used to specify input and output sources (like paths or addresses), system parameters (parallelism, runtime configuration), and application specific parameters (typically used within user functions).

Flink provides a simple utility called `ParameterTool` to provide some basic tooling for solving these problems.
Please note that you don't have to use the `ParameterTool` described here. Other frameworks such as [Commons CLI](https://commons.apache.org/proper/commons-cli/) and
[argparse4j](http://argparse4j.sourceforge.net/) also work well with Flink.


### Getting your configuration values into the `ParameterTool`

The `ParameterTool` provides a set of predefined static methods for reading the configuration. The tool is internally expecting a `Map<String, String>`, so it's very easy to integrate it with your own configuration style.


#### From `.properties` files

The following method will read a [Properties](https://docs.oracle.com/javase/tutorial/essential/environment/properties.html) file and provide the key/value pairs:
```java
String propertiesFilePath = "/home/sam/flink/myjob.properties";
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);

File propertiesFile = new File(propertiesFilePath);
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);

InputStream propertiesFileInputStream = new FileInputStream(file);
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFileInputStream);
```


#### From the command line arguments

This allows getting arguments like `--input hdfs:///mydata --elements 42` from the command line.
```java
public static void main(String[] args) {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    // .. regular code ..
```


#### From system properties

When starting a JVM, you can pass system properties to it: `-Dinput=hdfs:///mydata`. You can also initialize the `ParameterTool` from these system properties:

```java
ParameterTool parameter = ParameterTool.fromSystemProperties();
```


### Using the parameters in your Flink program

Now that we've got the parameters from somewhere (see above) we can use them in various ways.

**Directly from the `ParameterTool`**

The `ParameterTool` itself has methods for accessing the values.
```java
ParameterTool parameters = // ...
parameter.getRequired("input");
parameter.get("output", "myDefaultValue");
parameter.getLong("expectedCount", -1L);
parameter.getNumberOfParameters()
// .. there are more methods available.
```

You can use the return values of these methods directly in the `main()` method of the client submitting the application.
For example, you could set the parallelism of a operator like this:

```java
ParameterTool parameters = ParameterTool.fromArgs(args);
int parallelism = parameters.get("mapParallelism", 2);
DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).setParallelism(parallelism);
```

Since the `ParameterTool` is serializable, you can pass it to the functions itself:

```java
ParameterTool parameters = ParameterTool.fromArgs(args);
DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer(parameters));
```

and then use it inside the function for getting values from the command line.

#### Register the parameters globally

Parameters registered as global job parameters in the `ExecutionConfig` can be accessed as configuration values from the JobManager web interface and in all functions defined by the user.

Register the parameters globally:

```java
ParameterTool parameters = ParameterTool.fromArgs(args);

// set up the execution environment
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().setGlobalJobParameters(parameters);
```

Access them in any rich user function:

```java
public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
	ParameterTool parameters = (ParameterTool)
	    getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
	parameters.getRequired("input");
	// .. do more ..
```

{{< top >}}
