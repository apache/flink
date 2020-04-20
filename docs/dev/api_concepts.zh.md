---
title: "基础 API 概念"
nav-parent_id: dev
nav-pos: 1
nav-show_overview: true
nav-id: api-concepts
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

Flink 程序是实现了分布式集合转换（例如过滤、映射、更新状态、join、分组、定义窗口、聚合）的规范化程序。集合初始创建自 source（例如读取文件、kafka 主题，或本地内存中的集合）。结果通过 sink 返回，例如，它可以将数据写入（分布式）文件，或标准输出（例如命令行终端）。Flink 程序可以在多种环境中运行，独立运行或嵌入到其他程序中。可以在本地 JVM 中执行，也可以在多台机器的集群上执行。

针对有界和无界两种数据 source 类型，你可以使用 DataSet API 来编写批处理程序或使用 DataStream API 来编写流处理程序。本篇指南将介绍这两种 API 通用的基本概念，关于使用 API 编写程序的具体信息请查阅
[流处理指南]({{ site.baseurl }}/zh/dev/datastream_api.html) 和
[批处理指南]({{ site.baseurl }}/zh/dev/batch/index.html)。

**请注意：** 当展示如何使用 API 的实际示例时我们使用 `StreamingExecutionEnvironment` 和 `DataStream API`。对于批处理，将他们替换为 `ExecutionEnvironment` 和 `DataSet API` 即可，概念是完全相同的。

* This will be replaced by the TOC
{:toc}

DataSet 和 DataStream
----------------------

Flink 用特有的 `DataSet` 和 `DataStream` 类来表示程序中的数据。你可以将他们视为可能包含重复项的不可变数据集合。对于 `DataSet`，数据是有限的，而对于 `DataStream`，元素的数量可以是无限的。

这些集合与标准的 Java 集合有一些关键的区别。首先它们是不可变的，也就是说它们一旦被创建你就不能添加或删除元素了。你也不能简单地检查它们内部的元素。

在 Flink 程序中，集合最初通过添加数据 source 来创建，通过使用诸如 `map`、`filter` 等 API 方法对数据 source 进行转换从而派生新的集合。

剖析一个 Flink 程序
--------------------------

Flink 程序看起来像是转换数据集合的规范化程序。每个程序由一些基本的部分组成：

1. 获取执行环境，
2. 加载/创建初始数据，
3. 指定对数据的转换操作，
4. 指定计算结果存放的位置，
5. 触发程序执行


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">


我们现在将概述每个步骤，详细信息请参阅相应章节。请注意所有 Java DataSet API 的核心类可以在这个包 {% gh_link /flink-java/src/main/java/org/apache/flink/api/java "org.apache.flink.api.java" %} 中找到，同时 Java DataStream API 可以在这个包
{% gh_link /flink-streaming-java/src/main/java/org/apache/flink/streaming/api "org.apache.flink.streaming.api" %} 中找到。

`StreamExecutionEnvironment` 是所有 Flink 程序的基础。你可以使用它的这些静态方法获取：

{% highlight java %}
getExecutionEnvironment()

createLocalEnvironment()

createRemoteEnvironment(String host, int port, String... jarFiles)
{% endhighlight %}

通常你只需要使用 `getExecutionEnvironment()`，因为它会根据上下文环境完成正确的工作：如果你在 IDE 中执行程序或者作为标准的 Java 程序来执行，它会创建你的本机执行环境；如果你将程序封装成 JAR 包，然后通过[命令行]({{ site.baseurl }}/zh/ops/cli.html)调用，Flink 集群管理器会执行你的 main 方法并且 `getExecutionEnvironment()` 会返回在集群上执行程序的执行环境。

针对不同的数据 source，执行环境有若干不同读取文件的方法：你可以逐行读取 CSV 文件，或者使用完全自定义的输入格式。要将文本文件作为一系列行读取，你可以使用：

{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> text = env.readTextFile("file:///path/to/file");
{% endhighlight %}

这样你会得到一个 DataStream 然后对其应用转换操作从而创建新的派生 DataStream。

通过调用 DataStream 的转换函数来进行转换。下面是一个映射转换的实例：

{% highlight java %}
DataStream<String> input = ...;

DataStream<Integer> parsed = input.map(new MapFunction<String, Integer>() {
    @Override
    public Integer map(String value) {
        return Integer.parseInt(value);
    }
});
{% endhighlight %}

这会通过把原始数据集合的每个字符串转换为一个整数创建一个新的 DataStream。

一旦你得到了包含最终结果的 DataStream，就可以通过创建 sink 将其写入外部系统。如下是一些创建 sink 的示例：

{% highlight java %}
writeAsText(String path)

print()
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

我们现在将概述每个步骤，详细信息请参阅相应章节。请注意所有 Scala DataSet API 可以在这个包 {% gh_link /flink-scala/src/main/scala/org/apache/flink/api/scala "org.apache.flink.api.scala" %} 中找到，同时，所有 Scala DataStream API 可以在这个包 {% gh_link /flink-streaming-scala/src/main/scala/org/apache/flink/streaming/api/scala "org.apache.flink.streaming.api.scala" %} 中找到。

`StreamExecutionEnvironment` 是所有 Flink 程序的基础。你可以使用它的这些静态方法获取：

{% highlight scala %}
getExecutionEnvironment()

createLocalEnvironment()

createRemoteEnvironment(host: String, port: Int, jarFiles: String*)
{% endhighlight %}

通常你只需要使用 `getExecutionEnvironment()`，因为它会根据上下文环境完成正确的工作，如果你在 IDE 中执行程序或者作为标准的 Java 程序来执行，它会创建你的本机执行环境。如果你将程序封装成 JAR 包，然后通过[命令行]({{ site.baseurl }}/zh/ops/cli.html)调用，Flink 集群管理器会执行你的 main 方法并且 `getExecutionEnvironment()` 会返回在集群上执行程序的执行环境。

针对不同的数据 source，执行环境有若干不同的读取文件的方法：你可以逐行读取 CSV 文件，或者使用完全自定义的输入格式。要将文本文件作为一系列行读取，你可以使用：

{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()

val text: DataStream[String] = env.readTextFile("file:///path/to/file")
{% endhighlight %}

如此你会得到一个 DataStream 然后对其应用转换操作从而创建新的派生 DataStream。

通过调用 DataStream 的转换函数来进行转换。下面是一个映射转换的实例：

{% highlight scala %}
val input: DataSet[String] = ...

val mapped = input.map { x => x.toInt }
{% endhighlight %}

这会通过把原始数据集合的每个字符串转换为一个整数创建一个新的 DataStream。

一旦你得到了包含最终结果的 DataStream，就可以通过创建 sink 将其写入外部系统。如下是一些创建 sink 的示例：

{% highlight scala %}
writeAsText(path: String)

print()
{% endhighlight %}

</div>
</div>

当设定好整个程序以后你需要调用 `StreamExecutionEnvironment` 的 `execute()` 方法**触发程序执行**。至于在你的本机触发还是提交到集群运行取决于 `ExecutionEnvironment` 的类型。

`execute()` 方法返回 `JobExecutionResult`，它包括执行耗时和一个累加器的结果。

如果你不需要等待作业的结束，只是想要触发程序执行，你可以调用 `StreamExecutionEnvironment` 的 `executeAsync()` 方法。这个方法将返回一个 `JobClient` 对象，通过 `JobClient` 能够与程序对应的作业进行交互。作为例子，这里介绍通过 `executeAsync()` 实现与 `execute()` 相同行为的方法。

{% highlight java %}
final JobClient jobClient = env.executeAsync();

final JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult(userClassloader).get();
{% endhighlight %}

有关流数据的 source 和 sink 以及有关 DataStream 支持的转换操作的详细信息请参阅[流处理指南]({{ site.baseurl }}/zh/dev/datastream_api.html)。

有关批数据的 source 和 sink 以及有关 DataSet 支持的转换操作的详细信息请参阅[批处理指南]({{ site.baseurl }}/zh/dev/batch/index.html)。


{% top %}

延迟计算
---------------

无论在本地还是集群执行，所有的 Flink 程序都是延迟执行的：当程序的 main 方法被执行时，并不立即执行数据的加载和转换，而是创建每个操作并将其加入到程序的执行计划中。当执行环境调用 `execute()` 方法显式地触发执行的时候才真正执行各个操作。

延迟计算允许你构建复杂的程序，Flink 将其作为整体计划单元来执行。

{% top %}

指定键
---------------

一些转换操作（join, coGroup, keyBy, groupBy）要求在元素集合上定义键。另外一些转换操作 （Reduce, GroupReduce, Aggregate, Windows）允许在应用这些转换之前将数据按键分组。

如下对 DataSet 分组
{% highlight java %}
DataSet<...> input = // [...]
DataSet<...> reduced = input
  .groupBy(/*在这里定义键*/)
  .reduceGroup(/*一些处理操作*/);
{% endhighlight %}

如下对 DataStream 指定键
{% highlight java %}
DataStream<...> input = // [...]
DataStream<...> windowed = input
  .keyBy(/*在这里定义键*/)
  .window(/*指定窗口*/);
{% endhighlight %}

Flink 的数据模型不是基于键值对的。因此你不需要将数据集类型物理地打包到键和值中。键都是“虚拟的”：它们的功能是指导分组算子用哪些数据来分组。

**请注意：**下面的讨论中我们将以 `DataStream` 和 `keyby` 为例。
对于 DataSet API 你只需要用 `DataSet` 和 `groupBy` 替换即可。

### 为 Tuple 定义键
{:.no_toc}

最简单的方式是按照 Tuple 的一个或多个字段进行分组：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple3<Integer,String,Long>> input = // [...]
KeyedStream<Tuple3<Integer,String,Long>,Tuple> keyed = input.keyBy(0)
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[(Int, String, Long)] = // [...]
val keyed = input.keyBy(0)
{% endhighlight %}
</div>
</div>

按照第一个字段（整型字段）对 Tuple 分组。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple3<Integer,String,Long>> input = // [...]
KeyedStream<Tuple3<Integer,String,Long>,Tuple> keyed = input.keyBy(0,1)
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataSet[(Int, String, Long)] = // [...]
val grouped = input.groupBy(0,1)
{% endhighlight %}
</div>
</div>

这里我们用第一个字段和第二个字段组成的组合键对 Tuple 分组

对于嵌套 Tuple 请注意： 如果你的 DataStream 是嵌套 Tuple，例如：

{% highlight java %}
DataStream<Tuple3<Tuple2<Integer, Float>,String,Long>> ds;
{% endhighlight %}

指定 `keyBy(0)` 将导致系统使用整个 `Tuple2` 作为键（一个整数和一个浮点数）。 如果你想“进入”到 `Tuple2` 的内部，你必须使用如下所述的字段表达式键。

### 使用字段表达式定义键
{:.no_toc}

可以使用基于字符串的字段表达式来引用嵌套字段，并定义用于分组、排序、join 或 coGrouping 的键。

字段表达式可以很容易地选取复合（嵌套）类型中的字段，例如 [Tuple](#tuples-and-case-classes) 和 [POJO](#pojos) 类型。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

下例中，我们有一个包含“word”和“count”两个字段的 POJO：`WC`。要用 `word` 字段分组，我们只需要把它的名字传给 `keyBy()` 函数即可。
{% highlight java %}
// 普通的 POJO（简单的 Java 对象）
public class WC {
  public String word;
  public int count;
}
DataStream<WC> words = // [...]
DataStream<WC> wordCounts = words.keyBy("word").window(/*指定窗口*/);
{% endhighlight %}

**字段表达式语法**：

- 根据字段名称选择 POJO 的字段。例如 `“user”` 就是指 POJO 类型的“user”字段。

- 根据字段名称或 0 开始的字段索引选择 Tuple 的字段。例如 `“f0”` 和 `“5”` 分别指 Java Tuple 类型的第一个和第六个字段。

- 可以选择 POJO 和 Tuple 的嵌套字段。 例如，一个 POJO 类型有一个“user”字段还是一个 POJO 类型，那么 `“user.zip”` 即指这个“user”字段的“zip”字段。任意嵌套和混合的 POJO 和 Tuple都是支持的，例如 `“f1.user.zip”` 或 `“user.f3.1.zip”`。

- 可以使用 `"*"` 通配符表达式选择完整的类型。这也适用于非 Tuple 或 POJO 类型。

**字段表达式示例**:

{% highlight java %}
public static class WC {
  public ComplexNestedClass complex; //嵌套的 POJO
  private int count;
  // 私有字段（count）的 getter 和 setter
  public int getCount() {
    return count;
  }
  public void setCount(int c) {
    this.count = c;
  }
}
public static class ComplexNestedClass {
  public Integer someNumber;
  public float someFloat;
  public Tuple3<Long, Long, String> word;
  public IntWritable hadoopCitizen;
}
{% endhighlight %}

这些字段表达式对于以上代码示例都是合法的：

- `"count"`：`WC` 类的 count 字段。

- `"complex"`：递归选择 POJO 类型 `ComplexNestedClass` 的 complex 字段的全部字段。

- `"complex.word.f2"`：选择嵌套 `Tuple3` 类型的最后一个字段。

- `"complex.hadoopCitizen"`：选择 hadoop 的 `IntWritable` 类型。

</div>
<div data-lang="scala" markdown="1">

下例中，我们有一个包含“word”和“count”两个字段的 POJO：`WC`。要用 `word` 字段分组，我们只需要把它的名字传给 `keyBy()` 函数即可。
{% highlight scala %}
// 普通的 POJO（简单的 Java 对象）
class WC(var word: String, var count: Int) {
  def this() { this("", 0L) }
}
val words: DataStream[WC] = // [...]
val wordCounts = words.keyBy("word").window(/*指定窗口*/)

// 或者，代码少一点的 case class
case class WC(word: String, count: Int)
val words: DataStream[WC] = // [...]
val wordCounts = words.keyBy("word").window(/*指定窗口*/)
{% endhighlight %}

**字段表达式语法**：

- 根据字段名称选择 POJO 的字段。例如 `“user”` 就是指 POJO 类型的“user”字段。

- 根据 1 开始的字段名称或 0 开始的字段索引选择 Tuple 的字段。例如 `“_1”` 和 `“5”` 分别指 Java Tuple 类型的第一个和第六个字段。

- 可以选择 POJO 和 Tuple 的嵌套字段。 例如，一个 POJO 类型有一个“user”字段还是一个 POJO 类型，那么 `“user.zip”` 即指这个“user”字段的“zip”字段。任意嵌套和混合的 POJO 和 Tuple都是支持的，例如 `“_2.user.zip”` 或 `“user._4.1.zip”`。

- 可以使用 `"*"` 通配符表达式选择完整的类型。这也适用于非 Tuple 或 POJO 类型。

**字段表达式示例**:

{% highlight scala %}
class WC(var complex: ComplexNestedClass, var count: Int) {
  def this() { this(null, 0) }
}

class ComplexNestedClass(
    var someNumber: Int,
    someFloat: Float,
    word: (Long, Long, String),
    hadoopCitizen: IntWritable) {
  def this() { this(0, 0, (0, 0, ""), new IntWritable(0)) }
}
{% endhighlight %}

这些字段表达式对于以上代码示例都是合法的：

- `"count"`：`WC` 类的 count 字段。

- `"complex"`：递归选择 POJO 类型 `ComplexNestedClass` 的 complex 字段的全部字段。

- `"complex.word._3"`：选择嵌套 `Tuple3` 类型的最后一个字段。

- `"complex.hadoopCitizen"`：选择 hadoop 的 `IntWritable` 类型。

</div>
</div>

### 使用键选择器函数定义键
{:.no_toc}

定义键的另一种方法是“键选择器”函数。键选择器函数将单个元素作为输入并返回元素的键。键可以是任意类型，并且可以由确定性计算得出。

下例展示了一个简单返回对象字段的键选择器函数：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 普通的 POJO
public class WC {public String word; public int count;}
DataStream<WC> words = // [...]
KeyedStream<WC> keyed = words
  .keyBy(new KeySelector<WC, String>() {
     public String getKey(WC wc) { return wc.word; }
   });
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// 普通的 case class
case class WC(word: String, count: Int)
val words: DataStream[WC] = // [...]
val keyed = words.keyBy( _.word )
{% endhighlight %}
</div>
</div>

{% top %}

指定转换函数
--------------------------

大多数转换操作需要用户定义函数。本节列举了指定它们的不同方法。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

#### 实现接口

最基本的方法是实现一个已提供的接口：

{% highlight java %}
class MyMapFunction implements MapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
};
data.map(new MyMapFunction());
{% endhighlight %}

#### 匿名类

可以将函数作为匿名类传递：
{% highlight java %}
data.map(new MapFunction<String, Integer> () {
  public Integer map(String value) { return Integer.parseInt(value); }
});
{% endhighlight %}

#### Java 8 Lambda 表达式

Flink 的 Java API 也支持 Java 8 Lambda 表达式。

{% highlight java %}
data.filter(s -> s.startsWith("http://"));
{% endhighlight %}

{% highlight java %}
data.reduce((i1,i2) -> i1 + i2);
{% endhighlight %}

#### 富函数

所有需要用户定义函数的转换操作都可以将*富*函数作为参数。例如，对于

{% highlight java %}
class MyMapFunction implements MapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
};
{% endhighlight %}

你可以替换成

{% highlight java %}
class MyMapFunction extends RichMapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
};
{% endhighlight %}

并像往常一样将函数传递给 `map` 转换操作：

{% highlight java %}
data.map(new MyMapFunction());
{% endhighlight %}

富函数也可以被定义为匿名类：
{% highlight java %}
data.map (new RichMapFunction<String, Integer>() {
  public Integer map(String value) { return Integer.parseInt(value); }
});
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">


#### Lambda 函数

正如前面的例子中所见，所有操作都接受 lambda 函数来描述操作：
{% highlight scala %}
val data: DataSet[String] = // [...]
data.filter { _.startsWith("http://") }
{% endhighlight %}

{% highlight scala %}
val data: DataSet[Int] = // [...]
data.reduce { (i1,i2) => i1 + i2 }
// 或者
data.reduce { _ + _ }
{% endhighlight %}

#### 富函数

所有需要用户定义函数的转换操作都可以将*富*函数作为参数。例如，对于

{% highlight scala %}
data.map { x => x.toInt }
{% endhighlight %}

你可以替换成

{% highlight scala %}
class MyMapFunction extends RichMapFunction[String, Int] {
  def map(in: String):Int = { in.toInt }
};
{% endhighlight %}

并像往常一样将函数传递给 `map` 转换操作：

{% highlight scala %}
data.map(new MyMapFunction())
{% endhighlight %}

富函数也可以被定义为匿名类：
{% highlight scala %}
data.map (new RichMapFunction[String, Int] {
  def map(in: String):Int = { in.toInt }
})
{% endhighlight %}
</div>

</div>

富函数为用户定义函数（map、reduce 等）额外提供了 4 个方法： `open`、`close`、`getRuntimeContext` 和 `setRuntimeContext`。这些方法有助于向函数传参（请参阅 [向函数传递参数]({{ site.baseurl }}/zh/dev/batch/index.html#passing-parameters-to-functions)）、
创建和终止本地状态、访问广播变量（请参阅
[广播变量]({{ site.baseurl }}/zh/dev/batch/index.html#broadcast-variables)）、访问诸如累加器和计数器等运行时信息（请参阅
[累加器和计数器](#accumulators--counters)）和迭代信息（请参阅 [迭代]({{ site.baseurl }}/zh/dev/batch/iterations.html)）。

{% top %}

支持的数据类型
--------------------

Flink 对于 DataSet 或 DataStream 中可以包含的元素类型做了一些限制。这么做是为了使系统能够分析类型以确定有效的执行策略。

有七种不同的数据类型：

1. **Java Tuple** 和 **Scala Case Class**
2. **Java POJO**
3. **基本数据类型**
4. **常规的类**
5. **值**
6. **Hadoop Writable**
7. **特殊类型**

#### Tuple 和 Case Class

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Tuple 是复合类型，包含固定数量的各种类型的字段。
Java API 提供了从 `Tuple1` 到 `Tuple25` 的类。 Tuple 的每一个字段可以是任意 Flink 类型，包括 Tuple，即嵌套的 Tuple。Tuple 的字段可以通过字段名称如 `tuple.f4` 直接访问，也可以使用通常的 getter 方法 `tuple.getField(int position)`。字段索引从 0 开始。请注意，这与 Scala Tuple 形成鲜明对比，但与 Java 的常规索引更为一致。

{% highlight java %}
DataStream<Tuple2<String, Integer>> wordCounts = env.fromElements(
    new Tuple2<String, Integer>("hello", 1),
    new Tuple2<String, Integer>("world", 2));

wordCounts.map(new MapFunction<Tuple2<String, Integer>, Integer>() {
    @Override
    public Integer map(Tuple2<String, Integer> value) throws Exception {
        return value.f1;
    }
});

wordCounts.keyBy(0); // .keyBy("f0") 也可以


{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

Scala Case Class（以及作为 Case Class 的特例的 Scala Tuple）是复合类型，包含固定数量的各种类型的字段。Tuple 的字段从 1 开始索引。例如 `_1` 指第一个字段。Case Class 字段用名称索引。

{% highlight scala %}
case class WordCount(word: String, count: Int)
val input = env.fromElements(
    WordCount("hello", 1),
    WordCount("world", 2)) // Case Class 数据集

input.keyBy("word")// 以字段表达式“word”为键

val input2 = env.fromElements(("hello", 1), ("world", 2)) // Tuple2 数据集

input2.keyBy(0, 1) // 以第 0 和第 1 个字段为键
{% endhighlight %}

</div>
</div>

#### POJO

Flink 将满足如下条件的 Java 和 Scala 的类作为特殊的 POJO 数据类型处理：

- 类必须是公有的。

- 它必须有一个公有的无参构造器（默认构造器）。

- 所有的字段要么是公有的要么必须可以通过 getter 和 setter 函数访问。例如一个名为 `foo` 的字段，它的 getter 和 setter 方法必须命名为 `getFoo()` 和 `setFoo()`。

- 字段的类型必须被已注册的序列化程序所支持。

POJO 通常用 `PojoTypeInfo` 表示，并使用 `PojoSerializer`（[Kryo](https://github.com/EsotericSoftware/kryo) 作为可配置的备用序列化器）序列化。
例外情况是 POJO 是 Avro 类型（Avro 指定的记录）或作为“Avro 反射类型”生成时。
在这种情况下 POJO 由 `AvroTypeInfo` 表示，并且由 `AvroSerializer` 序列化。
如果需要，你可以注册自己的序列化器；更多信息请参阅 [序列化](https://ci.apache.org/projects/flink/flink-docs-master/zh/dev/types_serialization.html)。

Flink 分析 POJO 类型的结构，也就是说，它会推断出 POJO 的字段。因此，POJO 类型比常规类型更易于使用。此外，Flink 可以比一般类型更高效地处理 POJO。

下例展示了一个拥有两个公有字段的简单 POJO。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class WordWithCount {

    public String word;
    public int count;

    public WordWithCount() {}

    public WordWithCount(String word, int count) {
        this.word = word;
        this.count = count;
    }
}

DataStream<WordWithCount> wordCounts = env.fromElements(
    new WordWithCount("hello", 1),
    new WordWithCount("world", 2));

wordCounts.keyBy("word"); // 以字段表达式“word”为键

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
class WordWithCount(var word: String, var count: Int) {
    def this() {
      this(null, -1)
    }
}

val input = env.fromElements(
    new WordWithCount("hello", 1),
    new WordWithCount("world", 2)) // Case Class 数据集

input.keyBy("word")// 以字段表达式“word”为键

{% endhighlight %}
</div>
</div>

#### 基本数据类型

Flink 支持所有 Java 和 Scala 的基本数据类型如 `Integer`、 `String`、和 `Double`。

#### 常规的类

Flink 支持大部分 Java 和 Scala 的类（API 和自定义）。
除了包含无法序列化的字段的类，如文件指针，I / O流或其他本地资源。遵循 Java Beans 约定的类通常可以很好地工作。

Flink 对于所有未识别为 POJO 类型的类（请参阅上面对于的 POJO 要求）都作为常规类处理。
Flink 将这些数据类型视为黑盒，并且无法访问其内容（为了诸如高效排序等目的）。常规类使用 [Kryo](https://github.com/EsotericSoftware/kryo) 序列化框架进行序列化和反序列化。

#### 值

*值* 类型手工描述其序列化和反序列化。它们不是通过通用序列化框架，而是通过实现 `org.apache.flinktypes.Value` 接口的 `read` 和 `write` 方法来为这些操作提供自定义编码。当通用序列化效率非常低时，使用值类型是合理的。例如，用数组实现稀疏向量。已知数组大部分元素为零，就可以对非零元素使用特殊编码，而通用序列化只会简单地将所有数组元素都写入。

`org.apache.flinktypes.CopyableValue` 接口以类似的方式支持内部手工克隆逻辑。

Flink 有与基本数据类型对应的预定义值类型。（`ByteValue`、
`ShortValue`、 `IntValue`、`LongValue`、 `FloatValue`、`DoubleValue`、 `StringValue`、`CharValue`、
`BooleanValue`）。这些值类型充当基本数据类型的可变变体：它们的值可以改变，允许程序员重用对象并减轻垃圾回收器的压力。


#### Hadoop Writable

可以使用实现了 `org.apache.hadoop.Writable` 接口的类型。它们会使用 `write()` 和 `readFields()` 方法中定义的序列化逻辑。

#### 特殊类型

可以使用特殊类型，包括 Scala 的 `Either`、`Option` 和 `Try`。
Java API 有对 `Either` 的自定义实现。
类似于 Scala 的 `Either`，它表示一个具有 *Left* 或 *Right* 两种可能类型的值。
`Either` 可用于错误处理或需要输出两种不同类型记录的算子。

#### 类型擦除和类型推断

*请注意： 本节只与 Java 有关。*

Java 编译器在编译后抛弃了大量泛型类型信息。这在 Java 中被称作 *类型擦除*。它意味着在运行时，对象的实例已经不知道它的泛型类型了。例如 `DataStream<String>` 和 `DataStream<Long>` 的实例在 JVM 看来是一样的。

Flink 在准备程序执行时（程序的 main 方法被调用时）需要类型信息。Flink Java API 尝试重建以各种方式丢弃的类型信息，并将其显式存储在数据集和算子中。你可以通过 `DataStream.getType()` 获取数据类型。此方法返回 `TypeInformation` 的一个实例，这是 Flink 内部表示类型的方式。

类型推断有其局限性，在某些情况下需要程序员的“配合”。
这方面的示例是从集合创建数据集的方法，例如 `ExecutionEnvironment.fromCollection()`，你可以在这里传递一个描述类型的参数。 像 `MapFunction<I, O>` 这样的泛型函数同样可能需要额外的类型信息。

可以通过输入格式和函数实现 {% gh_link /flink-core/src/main/java/org/apache/flink/api/java/typeutils/ResultTypeQueryable.java "ResultTypeQueryable" %}
接口，以明确告知 API 其返回类型。 被调函数的*输入类型*通常可以通过先前操作的结果类型来推断。

{% top %}

累加器和计数器
---------------------------

累加器简单地由 **加法操作** 和 **最终累加结果**构成，可在作业结束后使用。

最简单的累加器是一个 **计数器**：你可以使用
```Accumulator.add(V value)``` 方法递增它。作业结束时 Flink 会合计（合并）所有的部分结果并发送给客户端。累加器在 debug 或者你想快速了解数据的时候非常有用。

Flink 目前有如下 **内置累加器**。它们每一个都实现了
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/Accumulator.java "Accumulator" %}
接口。

- {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/IntCounter.java "__IntCounter__" %},
  {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/LongCounter.java "__LongCounter__" %}
  和 {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/DoubleCounter.java "__DoubleCounter__" %}：
  有关使用计数器的示例，请参见下文。
- {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/Histogram.java "__Histogram__" %}:
  离散数量桶的直方图实现。在内部，它只是一个从整数到整数的映射。你可以用它计算值的分布，例如一个词频统计程序中每行词频的分布。

__如何使用累加器：__

首先你必须在要使用它的用户定义转换函数中创建累加器对象（下例为计数器）。

{% highlight java %}
private IntCounter numLines = new IntCounter();
{% endhighlight %}

其次，你必须注册累加器对象，通常在富函数的 ```open()``` 方法中。在这里你还可以定义名称。

{% highlight java %}
getRuntimeContext().addAccumulator("num-lines", this.numLines);
{% endhighlight %}

你现在可以在算子函数中的任何位置使用累加器，包括 ```open()``` 和
```close()``` 方法。

{% highlight java %}
this.numLines.add(1);
{% endhighlight %}

总体结果将存储在 ```JobExecutionResult``` 对象中，该对象是从执行环境的 `execute()` 方法返回的
（目前这仅在执行等待作业完成时才有效）。

{% highlight java %}
myJobExecutionResult.getAccumulatorResult("num-lines")
{% endhighlight %}

每个作业的所有累加器共享一个命名空间。 这样你就可以在作业的不同算子函数中使用相同的累加器。Flink 会在内部合并所有同名累加器。

关于累加器和迭代请注意： 目前，累加器的结果只有在整个作业结束以后才可用。我们还计划实现在下一次迭代中使前一次迭代的结果可用。你可以使用
{% gh_link /flink-java/src/main/java/org/apache/flink/api/java/operators/IterativeDataSet.java#L98 "Aggregators" %}
计算每次迭代的统计信息，并根据这些信息确定迭代何时终止。

__自定义累加器：__

要实现你自己的累加器，只需编写累加器接口的实现即可。如果你认为 Flink 应该提供你的自定义累加器，请创建 pull request。

你可以选择实现
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/Accumulator.java "Accumulator" %}
或者 {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/SimpleAccumulator.java "SimpleAccumulator" %}。

```Accumulator<V,R>``` 最灵活：它为要递增的值定义类型 ```V```，为最终结果定义类型 ```R```。例如对于 histogram，```V``` 是数字而 ```R``` 是 histogram。```SimpleAccumulator``` 则适用于两个类型相同的情况，例如计数器。

{% top %}
