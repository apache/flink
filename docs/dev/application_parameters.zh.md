---
title: "Handling Application Parameters"
nav-id: application_parameters
nav-show_overview: true
nav-parent_id: streaming
nav-pos: 50
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

处理应用程序参数
-------------------------------
大部分Flink的应用，包括批处理、流处理在内，都依赖于外部的配置参数。
都习惯于指定进和出的数据源（比如：路径或者地址），系统配置（比如：并行度、运行参数配置），特定应用程序参数（通常是与用户功能有关的使用）。

Flink提供了一个简单工具，名为`ParameterTool`，为解决这些问题提供了一些基本工具。
请注意你不一定要用此处描述的`ParameterTool`，其它的框架比如 [Commons CLI](https://commons.apache.org/proper/commons-cli/) 和
[argparse4j](http://argparse4j.sourceforge.net/) 在Flink中一样使用的很好。


### 将你应用的参数放入 `ParameterTool`

The `ParameterTool` 提供一组预定义的静态方法用来读取配置。这个工具内置了令人期待的`Map<String, String>`, 所有它非常容易和你使用的参数配置风格进行集成。


#### 来自 `.properties` files
 
以下方法将读取一个[Properties](https://docs.oracle.com/javase/tutorial/essential/environment/properties.html) 文件，同时提供 key/value pairs:
{% highlight java %}
String propertiesFilePath = "/home/sam/flink/myjob.properties";
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);

File propertiesFile = new File(propertiesFilePath);
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);

InputStream propertiesFileInputStream = new FileInputStream(file);
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFileInputStream);
{% endhighlight %}


#### 来自命令行参数

This allows getting arguments like `--input hdfs:///mydata --elements 42` from the command line.
{% highlight java %}
public static void main(String[] args) {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    // .. regular code ..
{% endhighlight %}


#### 来自系统配置

When starting a JVM, you can pass system properties to it: `-Dinput=hdfs:///mydata`. You can also initialize the `ParameterTool` from these system properties:

{% highlight java %}
ParameterTool parameter = ParameterTool.fromSystemProperties();
{% endhighlight %}


### 在你的Flink程序中使用参数

现在我们可以从上述方法中获取的参数以各种方式进行应用。

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

{% top %}
