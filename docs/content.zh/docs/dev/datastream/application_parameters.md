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

# 处理应用程序参数



处理应用程序参数
-------------------------------
几乎所有的 Flink 应用程序，包括批处理和流处理，都依赖于外部配置参数。
它们用于指定输入和输出来源（如路径或地址），系统参数（并行度，运行时配置）和应用程序特定的参数（通常在用户功能内使用）。

Flink 提供了一个名为 `ParameterTool` 的简单实用程序，作为解决这些问题的基本工具。
请注意，开发者不是必须使用此处描述的 `ParameterTool`。其他框架例如 [Commons CLI](https://commons.apache.org/proper/commons-cli/) 和 [argparse4j](http://argparse4j.sourceforge.net/) 也可以与 Flink 一起使用。

### 用 `ParameterTool` 读取配置值

### 将你的配置值放入 `ParameterTool`

`ParameterTool` 提供了一些预定义的静态方法来读取配置。该工具内部使用一个 `Map<String, String>`，因此将其与你自己的配置样式集成起来非常容易。

#### 配置值来自 `.properties` 文件

#### 从 `.properties` 文件中读取

以下方法将读取一个 [Properties](https://docs.oracle.com/javase/tutorial/essential/environment/properties.html) 文件并提供键值对：
```java
String propertiesFilePath = "/home/sam/flink/myjob.properties";
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);

File propertiesFile = new File(propertiesFilePath);
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);

InputStream propertiesFileInputStream = new FileInputStream(file);
ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFileInputStream);
```

#### 配置值来自命令行

#### 从命令行参数中读取

允许从命令行中获取像 `--input hdfs:///mydata --elements 42` 这样的参数。
```java
public static void main(String[] args) {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    // .. 常规代码 ..
```


#### 从系统属性中读取

启动 JVM 时，你可以将系统属性传递给它：`-Dinput=hdfs:///mydata`。你也可以使用系统属性初始化 `ParameterTool`：

```java
ParameterTool parameter = ParameterTool.fromSystemProperties();
```

### 在 Flink 程序中使用参数

### 在你的 Flink 程序中使用参数

现在我们已经获取了参数（见上文），接下来我们可以以各种方式来使用它们。

**直接从 `ParameterTool` 中获取**

`ParameterTool` 自身具有访问值的方法。
```java
ParameterTool parameters = // ...
parameter.getRequired("input");
parameter.get("output", "myDefaultValue");
parameter.getLong("expectedCount", -1L);
parameter.getNumberOfParameters()
// .. 还有更多可用方法。
```

你可以在提交应用程序的客户端的 `main()` 方法中直接使用这些方法的返回值。
例如，你可以像这样为算子设置并行度：

```java
ParameterTool parameters = ParameterTool.fromArgs(args);
int parallelism = parameters.get("mapParallelism", 2);
DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).setParallelism(parallelism);
```

由于 `ParameterTool` 是可序列化的，因此你可以将其传递给函数本身：

```java
ParameterTool parameters = ParameterTool.fromArgs(args);
DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer(parameters));
```

然后在函数内部使用它从命令行中获取值。

#### 全局注册参数

可以从 JobManager web 接口以及用户自定义的所有功能中，将在 `ExecutionConfig` 中注册为全局参数的参数作为配置值进行访问。

全局注册参数：

```java
ParameterTool parameters = ParameterTool.fromArgs(args);

// 设置执行环境
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().setGlobalJobParameters(parameters);
```

可以在任何 rich function 中访问它们：

```java
public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        ParameterTool parameters = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        parameters.getRequired("input");
        // .. 更多 ..
```

{{< top >}}
