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

# 应用程序参数处理

应用程序参数处理
-------------------------------
几乎所有的Flink应用程序，也就是批和流程序，都依赖于外部配置参数。这些配置参数指定输入和输出源（如路径或地址），系统参数（并行度，运行时配置）和应用程序特定参数（通常在用户函数中使用）。

Flink提供一个名为 `Parametertool` 的简单实用类，为解决以上问题提供了基本的工具。 这里请注意，此处描述的` parametertool` 并不是必须的。[Commons CLI](https://commons.apache.org/proper/commons-cli/) 和 [argparse4j](http://argparse4j.sourceforge.net/)等其他框架也与Flink兼容非常好。

### `ParameterTool` 读取配置值

`ParameterTool` 提供一组预定义的静态方法，用于读取配置信息。 该工具类内部实现倾向于使用` Map<string，string>`，因此很容易与我们自己的配置集成。


#### 配置值来自 `.properties` 文件

以下方法将读取 [Properties](https://docs.oracle.com/javase/tutorial/essential/environment/properties.html) 文件并解析出键/值对：

```java
String propertiesFilePath = "/home/sam/flink/myjob.properties";
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);

File propertiesFile = new File(propertiesFilePath);
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);

InputStream propertiesFileInputStream = new FileInputStream(file);
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFileInputStream);
```


#### 配置值来自命令行

该操作从命令行获取像 `--input hdfs:///mydata --elements 42` 的参数。

```java
public static void main(String[] args) {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    // .. regular code ..
```


#### 配置值来自系统属性

启动JVM时，可以将系统属性传递给JVM：`-Dinput=hdfs:///mydata`。还可以从这些系统属性初始化 `ParameterTool`：

```java
ParameterTool parameter = ParameterTool.fromSystemProperties();
```

### Flink程序中使用参数

现在我们已经从某处获得了参数（见上文），我们可以以各种方式使用它们。

**直接从`ParameterTool`获取**

`ParameterTool`本身具有访问配置值的方法。

```java
ParameterTool parameters = // ...
parameter.getRequired("input");
parameter.get("output", "myDefaultValue");
parameter.getLong("expectedCount", -1L);
parameter.getNumberOfParameters()
// .. there are more methods available.
```

你可以直接在提交应用程序时在客户端的 `main()` 方法中使用这些方法的返回值。例如，你可以这样设置算子的并行度：

```java
ParameterTool parameters = ParameterTool.fromArgs(args);
int parallelism = parameters.get("mapParallelism", 2);
DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).setParallelism(parallelism);
```

由于 `ParameterTool` 是序列化的，可以将其传递给函数本身：

```java
ParameterTool parameters = ParameterTool.fromArgs(args);
DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer(parameters));
```

然后在函数内使用它以获取命令行的值。

#### 全局注册参数

从 JobManager web 界面和用户定义的所有函数中可以以配置值的方式获取在 `ExecutionConfig` 中注册为全局作业参数。

在全局注册参数：

```java
ParameterTool parameters = ParameterTool.fromArgs(args);

// set up the execution environment
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().setGlobalJobParameters(parameters);
```
在任意富函数中访问参数：

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
