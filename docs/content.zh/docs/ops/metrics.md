---
title: "指标"
weight: 6
type: docs
aliases:
  - /zh/ops/metrics.html
  - /zh/apis/metrics.html
  - /zh/monitoring/metrics.html
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

# 指标

Flink 公开了一个指标系统，允许收集和公开指标给外部系统。

## 注册指标

你可以通过调用任意继承 [RichFunction]({{< ref "docs/dev/datastream/user_defined_functions" >}}#rich-functions) 
的用户函数 `getRuntimeContext().getMetricGroup()` 方法访问指标系统。
此方法返回一个 `MetricGroup` 对象，你可以在该对象上创建和注册新指标。

## 指标类型

Flink 支持计数器 `Counters`，量表 `Gauges`，直方图 `Histogram` 和仪表 `Meters`。

### 计数器 Counter

计数器 `Counter` 用于计数。当前值可以使用 `inc()/inc(long n)` 增加计数值或使用 `dec()/dec(long n)` 减小计数值。
你可以在 `MetricGroup` 中调用 `counter(String name)` 来创建和注册一个计数器。

{{< tabs "9612d275-bdda-4322-a01f-ae6da805e917" >}}
{{< tab "Java" >}}
```java
public class MyMapper extends RichMapFunction<String, String> {
  private transient Counter counter;

  @Override
  public void open(Configuration config) {
    this.counter = getRuntimeContext()
      .getMetricGroup()
      .counter("myCounter");
  }

  @Override
  public String map(String value) throws Exception {
    this.counter.inc();
    return value;
  }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class MyMapper extends RichMapFunction[String,String] {
  @transient private var counter: Counter = _

  override def open(parameters: Configuration): Unit = {
    counter = getRuntimeContext()
      .getMetricGroup()
      .counter("myCounter")
  }

  override def map(value: String): String = {
    counter.inc()
    value
  }
}
```
{{< /tab >}}
{{< /tabs >}}

或者你也可以使用自己的计数器实现：

{{< tabs "e2de1ea4-fad3-4619-b4ba-fe41af1bd25f" >}}
{{< tab "Java" >}}
```java
public class MyMapper extends RichMapFunction<String, String> {
  private transient Counter counter;

  @Override
  public void open(Configuration config) {
    this.counter = getRuntimeContext()
      .getMetricGroup()
      .counter("myCustomCounter", new CustomCounter());
  }

  @Override
  public String map(String value) throws Exception {
    this.counter.inc();
    return value;
  }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class MyMapper extends RichMapFunction[String,String] {
  @transient private var counter: Counter = _

  override def open(parameters: Configuration): Unit = {
    counter = getRuntimeContext()
      .getMetricGroup()
      .counter("myCustomCounter", new CustomCounter())
  }

  override def map(value: String): String = {
    counter.inc()
    value
  }
}
```
{{< /tab >}}
{{< /tabs >}}

### 量表 Gauge

量表 `Gauge` 根据需求可以提供任何类型的值。为了使用量表你必须首先创建一个实现 `org.apache.flink.metrics.Gauge` 
接口的类。返回值的类型没有限制。
你可以在 `MetricGroup` 中调用 `gauge(String name, Gauge gauge)` 来创建和注册量表。

{{< tabs "1457e63d-28c4-4dbd-b742-582fe88706bf" >}}
{{< tab "Java" >}}
```java
public class MyMapper extends RichMapFunction<String, String> {
  private transient int valueToExpose = 0;

  @Override
  public void open(Configuration config) {
    getRuntimeContext()
      .getMetricGroup()
      .gauge("MyGauge", new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return valueToExpose;
        }
      });
  }

  @Override
  public String map(String value) throws Exception {
    valueToExpose++;
    return value;
  }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
new class MyMapper extends RichMapFunction[String,String] {
  @transient private var valueToExpose = 0

  override def open(parameters: Configuration): Unit = {
    getRuntimeContext()
      .getMetricGroup()
      .gauge[Int, ScalaGauge[Int]]("MyGauge", ScalaGauge[Int]( () => valueToExpose ) )
  }

  override def map(value: String): String = {
    valueToExpose += 1
    value
  }
}
```
{{< /tab >}}
{{< /tabs >}}

请注意报告会将暴露的对象变成字符串，这意味着实现类需要一个有意义的 `toString()` 方法。

### 直方图 Histogram

直方图 `Histogram` 测量长值的分布。
你可以在 `MetricGroup` 中调用 `histogram(String name, Histogram histogram)` 来创建和注册一个直方图。

{{< tabs "f00bd80e-ce30-497c-aa1f-89f3b5f653a0" >}}
{{< tab "Java" >}}
```java
public class MyMapper extends RichMapFunction<Long, Long> {
  private transient Histogram histogram;

  @Override
  public void open(Configuration config) {
    this.histogram = getRuntimeContext()
      .getMetricGroup()
      .histogram("myHistogram", new MyHistogram());
  }

  @Override
  public Long map(Long value) throws Exception {
    this.histogram.update(value);
    return value;
  }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class MyMapper extends RichMapFunction[Long,Long] {
  @transient private var histogram: Histogram = _

  override def open(parameters: Configuration): Unit = {
    histogram = getRuntimeContext()
      .getMetricGroup()
      .histogram("myHistogram", new MyHistogram())
  }

  override def map(value: Long): Long = {
    histogram.update(value)
    value
  }
}
```
{{< /tab >}}
{{< /tabs >}}

Flink 没有为直方图提供默认的实现类，但提供了允许使用 Codahale/DropWizard 直方图的
{{<gh_link file="flink-metrics/flink-metrics-dropwizard/src/main/java/org/apache/flink/dropwizard/metrics/DropwizardHistogramWrapper.java" name="Wrapper">}}。
如果要使用此包装类，请在你的 `pom.xml` 中添加如下依赖:
```xml
<dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-metrics-dropwizard</artifactId>
      <version>{{< version >}}</version>
</dependency>
```

然后，你可以像这样注册 Codahale/DropWizard 直方图：

{{< tabs "bb87937e-afd3-40c3-9ef2-95bce0cbaeb7" >}}
{{< tab "Java" >}}
```java
public class MyMapper extends RichMapFunction<Long, Long> {
  private transient Histogram histogram;

  @Override
  public void open(Configuration config) {
    com.codahale.metrics.Histogram dropwizardHistogram =
      new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

    this.histogram = getRuntimeContext()
      .getMetricGroup()
      .histogram("myHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));
  }
  
  @Override
  public Long map(Long value) throws Exception {
    this.histogram.update(value);
    return value;
  }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class MyMapper extends RichMapFunction[Long, Long] {
  @transient private var histogram: Histogram = _

  override def open(config: Configuration): Unit = {
    com.codahale.metrics.Histogram dropwizardHistogram =
      new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))
        
    histogram = getRuntimeContext()
      .getMetricGroup()
      .histogram("myHistogram", new DropwizardHistogramWrapper(dropwizardHistogram))
  }
  
  override def map(value: Long): Long = {
    histogram.update(value)
    value
  }
}
```
{{< /tab >}}
{{< /tabs >}}

### 仪表 Meter

仪表 `Meter` 测量的是平均吞吐量。一个事件的发生率可以用 `markEvent()` 方法来注册。多个事件的发生率可以用 `markEvent(long n)` 方法注册。
你可以在 `MetricGroup` 中调用 `meter(String name, Meter meter)` 来创建和注册一个仪表。

{{< tabs "39036212-06d1-4efe-bab3-d821aa11f6fe" >}}
{{< tab "Java" >}}
```java
public class MyMapper extends RichMapFunction<Long, Long> {
  private transient Meter meter;

  @Override
  public void open(Configuration config) {
    this.meter = getRuntimeContext()
      .getMetricGroup()
      .meter("myMeter", new MyMeter());
  }

  @Override
  public Long map(Long value) throws Exception {
    this.meter.markEvent();
    return value;
  }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class MyMapper extends RichMapFunction[Long,Long] {
  @transient private var meter: Meter = _

  override def open(config: Configuration): Unit = {
    meter = getRuntimeContext()
      .getMetricGroup()
      .meter("myMeter", new MyMeter())
  }

  override def map(value: Long): Long = {
    meter.markEvent()
    value
  }
}
```
{{< /tab >}}
{{< /tabs >}}

Flink 提供了一个
{{<gh_link file="flink-metrics/flink-metrics-dropwizard/src/main/java/org/apache/flink/dropwizard/metrics/DropwizardMeterWrapper.java" name="Wrapper">}}
，允许使用 Codahale/DropWizard 仪表。
要使用此仪表，请在你的 `pom.xml` 中添加如下依赖:

```xml
<dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-metrics-dropwizard</artifactId>
      <version>{{< version >}}</version>
</dependency>
```

你可以像这样注册 Codahale/DropWizard 仪表：

{{< tabs "9cc57972-cf86-401e-a394-ee97efd816f2" >}}
{{< tab "Java" >}}
```java
public class MyMapper extends RichMapFunction<Long, Long> {
  private transient Meter meter;

  @Override
  public void open(Configuration config) {
    com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();

    this.meter = getRuntimeContext()
      .getMetricGroup()
      .meter("myMeter", new DropwizardMeterWrapper(dropwizardMeter));
  }

  @Override
  public Long map(Long value) throws Exception {
    this.meter.markEvent();
    return value;
  }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class MyMapper extends RichMapFunction[Long,Long] {
  @transient private var meter: Meter = _

  override def open(config: Configuration): Unit = {
    val dropwizardMeter: com.codahale.metrics.Meter = new com.codahale.metrics.Meter()
  
    meter = getRuntimeContext()
      .getMetricGroup()
      .meter("myMeter", new DropwizardMeterWrapper(dropwizardMeter))
  }

  override def map(value: Long): Long = {
    meter.markEvent()
    value
  }
}
```
{{< /tab >}}
{{< /tabs >}}

## 范围

每个 metric 都分配有一个标识符和一组键值对，指标将在这些键值对下进行体现。

该标识符基于 3 个组件：注册指标时的用户定义名称、可选的用户定义范围和系统提供的范围。
例如，如果 `A.B` 是系统范围，`C.D` 是用户范围，`E` 是名称，则指标的标识符将为 `A.B.C.D.E`。

你可以在 `conf/flink-conf.yaml` 中设置 `metrics.scope.delimiter` 来配置用于标识符的分隔符（默认：`.`）。

### 用户范围

你可以通过调用 `MetricGroup#addGroup(String name)`， `MetricGroup#addGroup(int name)` 或者 
`MetricGroup#addGroup(String key, String value)` 来定义一个用户范围。
这些方法会影响 `MetricGroup#getMetricIdentifier` 和 `MetricGroup#getScopeComponents` 的返回值。

{{< tabs "8ba6943e-ab5d-45ce-8a73-091a01370eaf" >}}
{{< tab "Java" >}}
```java
counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetrics")
  .counter("myCounter");

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetricsKey", "MyMetricsValue")
  .counter("myCounter");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetrics")
  .counter("myCounter")

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetricsKey", "MyMetricsValue")
  .counter("myCounter")
```
{{< /tab >}}
{{< /tabs >}}

### 系统范围

系统范围包含关于指标的上下文信息，例如在哪个任务中注册的，或者该任务属于哪个作业。

可以通过在 `conf/flink-conf.yaml` 中设置以下键来配置应该包括哪些上下文信息。
每个键都期望一个可能包含常量（如 "taskmanager" ）和变量（如 "<task_id>" ）的标准化字符串，将在运行时生效。

- `metrics.scope.jm`
  - 默认值: &lt;host&gt;.jobmanager
  - 应用于范围在作业管理器上的所有指标。
- `metrics.scope.jm.job`
  - 默认值: &lt;host&gt;.jobmanager.&lt;job_name&gt;
  - 应用于范围在作业管理器和作业上的所有指标。
- `metrics.scope.tm`
  - 默认值: &lt;host&gt;.taskmanager.&lt;tm_id&gt;
  - 应用于范围在任务管理器的所有指标。
- `metrics.scope.tm.job`
  - 默认值: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;
  - 应用于范围在任务管理器和作业上的所有指标。
- `metrics.scope.task`
  - 默认值: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;task_name&gt;.&lt;subtask_index&gt;
  - 应用于范围在任务上的所有指标。
- `metrics.scope.operator`
  - 默认值: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;operator_name&gt;.&lt;subtask_index&gt;
  - 应用于范围在算子上的所有指标。

对变量的数量和顺序没有限制，变量是区分大小写的。

默认的指标范围将生成一个类似于 `localhost.taskmanager.1234.MyJob.MyOperator.0.MyMetric` 的标识。

如果你想包括任务名称但省略任务管理器的信息，你可以指定以下格式。

`metrics.scope.operator:<host>.<job_name>.<task_name>.<operator_name>.<subtask_index>`

这可以创建标识符 `localhost.MyJob.MySource_->_MyOperator.MyOperator.0.MyMetric`。

请注意，对于这种格式的字符串，如果同一个作业同时运行多次，可能会发生标识符冲突，这可能导致指标数据不一致。
因此，建议使用格式化字符串，通过包括ID（例如 <job_id> ）或为作业和操作者分配唯一的名称，提供一定程度的唯一性。

### 所有变量的列表

- JobManager: &lt;host&gt;
- TaskManager: &lt;host&gt;, &lt;tm_id&gt;
- Job: &lt;job_id&gt;, &lt;job_name&gt;
- Task: &lt;task_id&gt;, &lt;task_name&gt;, &lt;task_attempt_id&gt;, &lt;task_attempt_num&gt;, &lt;subtask_index&gt;
- Operator: &lt;operator_id&gt;,&lt;operator_name&gt;, &lt;subtask_index&gt;

**重要:** 对于批处理 API &lt;operator_id&gt; 总是等于 &lt;task_id&gt;。

### 用户变量

你可以通过调用 `MetricGroup#addGroup(String key, String value)` 来定义一个用户变量。
这个方法会影响 `MetricGroup#getMetricIdentifier`， `MetricGroup#getScopeComponents` 和 `MetricGroup#getAllVariables()` 的返回值。

**重要:** 用户变量不能用于标准范围。

{{< tabs "66c0ba7f-adc3-4a8b-831f-b0126ea2de81" >}}
{{< tab "Java" >}}
```java
counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetricsKey", "MyMetricsValue")
  .counter("myCounter");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetricsKey", "MyMetricsValue")
  .counter("myCounter")
```
{{< /tab >}}
{{< /tabs >}}

## 报告器

关于如何设置 Flink 的指标报告器的信息，请看[指标报告器文档]({{< ref "docs/deployment/metric_reporters">}})。

## 系统指标

默认情况下，Flink 会收集一些指标以深入了解当前的状态。本节是所有指标的参考。

下面的表格一般有 5 列。

* `Scope` 列描述了使用哪种范围格式来生成系统范围。
  例如，如果单元格包含"算子"，那么就使用 `metrics.scope.operator` 的范围格式。
  如果单元格包含多个值，用斜线隔开，那么指标就会为不同的实体报告多次，比如作业管理器和任务管理器。
* （可选的）`Infix` 列描述了哪个 infix 被附加到系统范围上。
* `Metrics` 列列出了为给定范围和infix注册的所有指标的名称。
* `Description` 列提供了关于一个给定指标测量的内容信息。
* `Type` 列描述了用于测量的指标类型。

注意，infix/metric 名称列中的所有点仍然受 `metrics.delimiter` 设置的制约。

因此，为了推断出指标标识符：

1、根据 `Scope` 列中的范围格式取值。 

2、如果存在的话，在 `Infix` 列中添加数值，并考虑到 `metrics.delimiter` 的设置。 

3、添加指标名称。

### 中央处理器
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 22%">中缀</th>
      <th class="text-left" style="width: 20%">指标</th>
      <th class="text-left" style="width: 32%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>Job-/TaskManager</strong></th>
      <td rowspan="2">Status.JVM.CPU</td>
      <td>Load</td>
      <td>JVM 最近的 CPU 使用率。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Time</td>
      <td>JVM 使用的 CPU 时间。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### 内存
与内存有关的指标需要 Oracle 的内存管理（也包括在 OpenJDK 的 Hotspot 中）。
当使用其他 JVM 实现（例如 IBM 的 J9 ）时，一些指标可能不会被公开。
<table class="table table-bordered">                               
  <thead>                                                          
    <tr>                                                           
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 22%">中缀</th>          
      <th class="text-left" style="width: 20%">指标</th>                           
      <th class="text-left" style="width: 32%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>                       
    </tr>                                                          
  </thead>                                                         
  <tbody>                                                          
    <tr>                                                           
      <th rowspan="17"><strong>Job-/TaskManager</strong></th>
      <td rowspan="15">Status.JVM.Memory</td>
      <td>Heap.Used</td>
      <td>当前使用的堆内存的数量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Heap.Committed</td>
      <td>保证 JVM 可用的堆内存的数量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Heap.Max</td>
      <td>可用于内存管理的堆内存的最大数量（以字节计）。这个值不一定等于通过 -Xmx 或同等的 Flink 
            配置参数指定的最大值。一些 GC 算法分配的堆内存不会被用户代码使用，因此不会通过堆指标暴露。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>NonHeap.Used</td>
      <td>当前使用的非堆内存的数量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>NonHeap.Committed</td>
      <td>保证提供给 JVM 的非堆内存的数量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>NonHeap.Max</td>
      <td>可以用于内存管理的非堆内存的最大数量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Metaspace.Used</td>
      <td>当前在 Metaspace 内存池中使用的内存量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Metaspace.Committed</td>
      <td>在 Metaspace 内存池中保证提供给 JVM 的内存量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Metaspace.Max</td>
      <td>在 Metaspace 内存池中可以使用的最大内存量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Direct.Count</td>
      <td>直接缓冲区池中的缓冲区数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Direct.MemoryUsed</td>
      <td>JVM用于直接缓冲池的内存量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Direct.TotalCapacity</td>
      <td>直接缓冲池中所有缓冲区的总容量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Mapped.Count</td>
      <td>映射缓冲池中缓冲区的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Mapped.MemoryUsed</td>
      <td>JVM 用于映射缓冲池的内存量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Mapped.TotalCapacity</td>
      <td>映射的缓冲池中的缓冲区数量（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="2">Status.Flink.Memory</td>
      <td>Managed.Used</td>
      <td>当前使用的托管内存的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Managed.Total</td>
      <td>管理的内存的总量。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### 线程
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 22%">中缀</th>
      <th class="text-left" style="width: 20%">指标</th>
      <th class="text-left" style="width: 32%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1"><strong>Job-/TaskManager</strong></th>
      <td rowspan="1">Status.JVM.Threads</td>
      <td>Count</td>
      <td>存活的线程总量。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### 垃圾收集器
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 22%">中缀</th>
      <th class="text-left" style="width: 20%">指标</th>
      <th class="text-left" style="width: 32%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>Job-/TaskManager</strong></th>
      <td rowspan="2">Status.JVM.GarbageCollector</td>
      <td>&lt;GarbageCollector&gt;.Count</td>
      <td>已经生成的集合的总数。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>&lt;GarbageCollector&gt;.Time</td>
      <td>执行垃圾回收的总时间。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### 类加载器
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 22%">中缀</th>
      <th class="text-left" style="width: 20%">指标</th>
      <th class="text-left" style="width: 32%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>Job-/TaskManager</strong></th>
      <td rowspan="2">Status.JVM.ClassLoader</td>
      <td>ClassesLoaded</td>
      <td>自 JVM 启动以来加载的类的总数。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>ClassesUnloaded</td>
      <td>自 JVM 启动以来卸载的类的总数。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>


### 网络

{{< hint warning >}}
废弃：使用[默认的 shuffle 处理指标](#默认的-shuffle-处理指标)
{{< /hint >}}

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 22%">中缀</th>
      <th class="text-left" style="width: 22%">指标</th>
      <th class="text-left" style="width: 30%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>TaskManager</strong></th>
      <td rowspan="2">Status.Network</td>
      <td>AvailableMemorySegments</td>
      <td>未使用的内存段的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>TotalMemorySegments</td>
      <td>已分配的内存段的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="10">Task</th>
      <td rowspan="6">buffers</td>
      <td>inputQueueLength</td>
      <td>排队的输入缓冲区的数量。（忽略正在使用阻塞子分区的本地输入通道）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>outputQueueLength</td>
      <td>排队的输出缓冲区的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inPoolUsage</td>
      <td>对输入缓冲区使用的估计。（忽略本地输入通道）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inputFloatingBuffersUsage</td>
      <td>对浮动输入缓冲区使用量的估计。（忽略本地输入通道）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inputExclusiveBuffersUsage</td>
      <td>对排他性输入缓冲区使用量的估计。（忽略本地输入通道）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>outPoolUsage</td>
      <td>对输出缓冲区使用情况的估计。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="4">Network.&lt;Input|Output&gt;.&lt;gate|partition&gt;<br />
        <strong>（只有在设置了 <code>taskmanager.net.detailed-metrics</code> 的时候才可用）</strong></td>
      <td>totalQueueLen</td>
      <td>所有输入/输出通道中排队的缓冲区总数。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>minQueueLen</td>
      <td>所有输入/输出通道中排队的缓冲区的最小数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>maxQueueLen</td>
      <td>所有输入/输出通道中排队的缓冲区的最大数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>avgQueueLen</td>
      <td>所有输入/输出通道中排队的缓冲区的平均数量。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### 默认的 shuffle 处理指标

与使用网状网络通信的任务执行者之间的数据交换有关的度量。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 22%">中缀</th>
      <th class="text-left" style="width: 22%">指标</th>
      <th class="text-left" style="width: 30%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="6"><strong>TaskManager</strong></th>
      <td rowspan="6">Status.Shuffle.Netty</td>
      <td>AvailableMemorySegments</td>
      <td>未使用的内存段的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>UsedMemorySegments</td>
      <td>已使用的内存段的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>TotalMemorySegments</td>
      <td>分配的内存段的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>AvailableMemory</td>
      <td>以字节为单位的未使用的内存量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>UsedMemory</td>
      <td>以字节为单位的已使用的内存量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>TotalMemory</td>
      <td>以字节为单位的已分配内存的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="18">Task</th>
      <td rowspan="4">Shuffle.Netty.Input.Buffers</td>
      <td>inputQueueLength</td>
      <td>排队的输入缓冲区的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inPoolUsage</td>
      <td>对输入缓冲区使用量的估计。（忽略本地输入通道）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inputFloatingBuffersUsage</td>
      <td>浮动输入缓冲区使用量的估计值。（忽略本地输入通道）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inputExclusiveBuffersUsage</td>
      <td>对排他性输入缓冲区使用量的估计。（忽略本地输入通道）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="2">Shuffle.Netty.Output.Buffers</td>
      <td>outputQueueLength</td>
      <td>排队的输出缓冲区的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>outPoolUsage</td>
      <td>对输出缓冲区使用情况的估计。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="4">Shuffle.Netty.&lt;Input|Output&gt;.&lt;gate|partition&gt;<br />
        <strong>（只有在设置了 <code>taskmanager.net.detailed-metrics</code> 的时候才可用）</strong></td>
      <td>totalQueueLen</td>
      <td>所有输入/输出通道中排队的缓冲区总数。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>minQueueLen</td>
      <td>所有输入/输出通道中排队的缓冲区的最小数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>maxQueueLen</td>
      <td>所有输入/输出通道中排队的缓冲区的最大数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>avgQueueLen</td>
      <td>所有输入/输出通道中排队的缓冲区的平均数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="8">Shuffle.Netty.Input</td>
      <td>numBytesInLocal</td>
      <td>该任务从本地源读取的总字节数。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesInLocalPerSecond</td>
      <td>该任务每秒从本地源读取的总字节数。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBytesInRemote</td>
      <td>该任务从远程源读取的总字节数。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesInRemotePerSecond</td>
      <td>该任务每秒从远程源读取的总字节数。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersInLocal</td>
      <td>该任务从本地源读取的网络缓冲区总数。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersInLocalPerSecond</td>
      <td>该任务每秒从本地源读取的网络缓冲区数量。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersInRemote</td>
      <td>该任务从远程源读取的网络缓冲区的总数。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersInRemotePerSecond</td>
      <td>该任务每秒从远程源读取的网络缓冲区数量。</td>
      <td>Meter</td>
    </tr>
  </tbody>
</table>

### 客户端
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 26%">指标</th>
      <th class="text-left" style="width: 48%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="4"><strong>JobManager</strong></th>
      <td>numRegisteredTaskManagers</td>
      <td>注册的任务管理器的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numRunningJobs</td>
      <td>运行的任务管理器的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>taskSlotsAvailable</td>
      <td>可用的任务槽的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>taskSlotsTotal</td>
      <td>任务槽的总数量。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### 可用性

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 26%">指标</th>
      <th class="text-left" style="width: 48%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5"><strong>Job（只有在 JobManager 上可用）</strong></th>
      <td>restartingTime</td>
      <td>重启工作所花的时间，或者说当前重启工作已经进行了多长时间（以毫秒为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>uptime</td>
      <td>作业不间断运行的时间（以毫秒为单位）。对于已完成的作业返回-1。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>downtime</td>
      <td>对于当前处于故障/恢复状态的作业，在此中断期间的耗时（以毫秒为单位）。正在运行的作业返回0，已完成的作业返回1。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>fullRestarts</td>
      <td><span class="label label-danger">注意：</span> 废弃，使用 <b>numRestarts</b>。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numRestarts</td>
      <td>自此作业提交以来，重新启动的总次数，包括完全重新启动和细粒度重新启动。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### 检查点

注意，对于失败的检查点，指标的更新是在尽力而为的基础上进行的，可能并不准确。
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 26%">指标</th>
      <th class="text-left" style="width: 48%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="8"><strong>Job（只有在 JobManager 上可用）</strong></th>
      <td>lastCheckpointDuration</td>
      <td>完成最后一个检查点的时间（以毫秒为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastCheckpointSize</td>
      <td>上次检查点的总大小（以字节为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastCheckpointExternalPath</td>
      <td>最后一个外部检查点的存储路径。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastCheckpointRestoreTimestamp</td>
      <td>最后一个检查点在协调器处被恢复的时间戳（以毫秒为单位）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numberOfInProgressCheckpoints</td>
      <td>正在进行中的检查点的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numberOfCompletedCheckpoints</td>
      <td>成功完成的检查点的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numberOfFailedCheckpoints</td>
      <td>失败的检查点的数量。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>totalNumberOfCheckpoints</td>
      <td>总的检查点的数量（正在进行的、已完成的、失败的）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="2"><strong>Task</strong></th>
      <td>checkpointAlignmentTime</td>
      <td>
        上一个标记对齐完成的时间（纳秒），或者当前对齐至今所花的时间（纳秒）。这是接收第一个和最后一个检查点对齐之间的时间。你可以在<a href="{{< ref "docs/ops/state/large_state_tuning">}}#monitoring-state-and-checkpoints">监控状态和检查点</a>部分找到更多信息。
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>checkpointStartDelayNanos</td>
      <td>从创建最后一个检查点到该任务启动检查点进程的时间（以纳秒为单位）。这个延迟显示了第一个检查点标记到达任务的时间。高的值表示产生了背压。如果只有一个特定的任务有很长的启动延迟，最可能的原因是数据倾斜。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### RocksDB
某些 RocksDB 本地指标是可用的，但默认是禁用的，你可以在[这里]({{< ref "docs/deployment/config" >}}#rocksdb-native-metrics)找到完整的文档。

### IO
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">范围</th>
      <th class="text-left" style="width: 26%">指标</th>
      <th class="text-left" style="width: 48%">描述</th>
      <th class="text-left" style="width: 8%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1"><strong>Job（只有在 JobManager 上可用）</strong></th>
      <td>[&lt;source_id&gt;.[&lt;source_subtask_index&gt;.]]&lt;operator_id&gt;.&lt;operator_subtask_index&gt;.latency</td>
      <td>从一个给定的源（子任务）到操作员子任务的延迟分布（以毫秒为单位），取决于<a href="{{< ref "docs/deployment/config" >}}#metrics-latency-granularity">延迟颗粒度</a>。</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <th rowspan="16"><strong>Task</strong></th>
      <td>numBytesInLocal</td>
      <td><span class="label label-danger">Attention:</span> 废弃，使用<a href="{{< ref "docs/ops/metrics" >}}#默认的-shuffle-处理指标">默认 shuffle 服务指标</a>。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesInLocalPerSecond</td>
      <td><span class="label label-danger">Attention:</span> 废弃，使用<a href="{{< ref "docs/ops/metrics" >}}#默认的-shuffle-处理指标">默认 shuffle 服务指标</a>。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBytesInRemote</td>
      <td><span class="label label-danger">Attention:</span> 废弃，使用<a href="{{< ref "docs/ops/metrics" >}}#默认的-shuffle-处理指标">默认 shuffle 服务指标</a>。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesInRemotePerSecond</td>
      <td><span class="label label-danger">Attention:</span> 废弃，使用<a href="{{< ref "docs/ops/metrics" >}}#默认的-shuffle-处理指标">默认 shuffle 服务指标</a>。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersInLocal</td>
      <td><span class="label label-danger">Attention:</span> 废弃，使用<a href="{{< ref "docs/ops/metrics" >}}#默认的-shuffle-处理指标">默认 shuffle 服务指标</a>。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersInLocalPerSecond</td>
      <td><span class="label label-danger">Attention:</span> 废弃，使用<a href="{{< ref "docs/ops/metrics" >}}#默认的-shuffle-处理指标">默认 shuffle 服务指标</a>。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersInRemote</td>
      <td><span class="label label-danger">Attention:</span> 废弃，使用<a href="{{< ref "docs/ops/metrics" >}}#默认的-shuffle-处理指标">默认 shuffle 服务指标</a>。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersInRemotePerSecond</td>
      <td><span class="label label-danger">Attention:</span> 废弃，使用<a href="{{< ref "docs/ops/metrics" >}}#默认的-shuffle-处理指标">默认 shuffle 服务指标</a>。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBytesOut</td>
      <td>这个任务发送的总字节数。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesOutPerSecond</td>
      <td>这个任务每秒钟发送的字节数。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersOut</td>
      <td>这个任务所发出的网络缓冲区的总数。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersOutPerSecond</td>
      <td>这个任务每秒发出的网络缓冲区的数量。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>isBackPressured</td>
      <td>该任务是否有背压。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>idleTimeMsPerSecond</td>
      <td>这个任务每秒空闲（没有数据要处理）的时间（以毫秒计）。闲置时间不包括背压时间，所以如果任务被背压就不是闲置。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>backPressuredTimeMsPerSecond</td>
      <td>该任务每秒背压的时间（以毫秒计）。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>busyTimeMsPerSecond</td>
      <td>该任务每秒忙碌（既非空闲也非背压）的时间（以毫秒为单位）。如果该值无法计算可以是NaN。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td rowspan="2"><strong>Task（仅当缓冲区去浮动功能被启用，并且在非源任务中）</strong></td>
      <td>estimatedTimeToConsumeBuffersMs</td>
      <td>缓冲区去势器处理该任务之前的网络交换中缓冲数据的估计时间（以毫秒为单位）。这个值是由传输中的数据的近似量和计算出的吞吐量计算出来的。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>debloatedBufferSize</td>
      <td>缓冲区去势器计算出的期望缓冲区大小（字节）。当传输中的数据量（考虑到当前的吞吐量后）超过配置的目标值时，缓冲区去势器就会试图减少缓冲区的大小。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="6"><strong>Task/Operator</strong></th>
      <td>numRecordsIn</td>
      <td>操作算子/任务收到的记录总数。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numRecordsInPerSecond</td>
      <td>操作算子/任务每秒收到的记录数。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numRecordsOut</td>
      <td>操作算子/任务已发出的记录总数。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numRecordsOutPerSecond</td>
      <td>操作算子/任务每秒发送的记录数。</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numLateRecordsDropped</td>
      <td>操作算子/任务因晚到而放弃的记录数。</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>currentInputWatermark</td>
      <td>操作算子/任务最后收到的水印（以毫秒为单位）。<p><strong>注意:</strong> 对于有 2 个输入的操作算子/任务，这是最后收到的水印的最小值。</p></td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="3"><strong>Operator</strong></th>
      <td>currentInput<strong>N</strong>Watermark</td>
      <td>
        该操作算子在其第 <strong>N</strong> 个输入中收到的最后一个水印（以毫秒计），索引 <strong>N</strong> 从 1 开始。例如 currentInput<strong>1</strong>Watermark， currentInput<strong>2</strong>Watermark，...
        <p><strong>注意：</strong> 只适用于有 2 个或更多输入的操作算子。</p>
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>currentOutputWatermark</td>
      <td>操作算子发出的最后一个水印（以毫秒计）。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numSplitsProcessed</td>
      <td>这个数据源处理过的输入切片的总数（如果操作算子是一个数据源）。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### 连接器

#### Kafka 连接器
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">范围</th>
      <th class="text-left" style="width: 18%">指标</th>
      <th class="text-left" style="width: 18%">用户变量</th>
      <th class="text-left" style="width: 39%">描述</th>
      <th class="text-left" style="width: 10%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1">Operator</th>
      <td>commitsSucceeded</td>
      <td>n/a</td>
      <td>如果偏移提交被打开并且检查点被启用，向 Kafka 成功偏移提交的总次数。</td>
      <td>Counter</td>
    </tr>
    <tr>
       <th rowspan="1">Operator</th>
       <td>commitsFailed</td>
       <td>n/a</td>
       <td>如果偏移提交被打开且检查点被启用，向 Kafka 提交偏移失败的总次数。请注意，将偏移量提交回 Kafka 只是暴露消费者进度的一种手段，所以提交失败并不影响 Flink 的检查点分区偏移量的完整性。</td>
       <td>Counter</td>
    </tr>
    <tr>
       <th rowspan="1">Operator</th>
       <td>committedOffsets</td>
       <td>topic, partition</td>
       <td>每个分区最后成功提交到 Kafka 的偏移量。一个特定分区的度量可以通过主题名称和分区 ID 来指定。</td>
       <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>currentOffsets</td>
      <td>topic, partition</td>
      <td>每个分区最后成功提交到 Kafka 的偏移量。一个特定分区的度量可以通过主题名称和分区 ID 来指定。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

#### Kinesis 连接器
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">范围</th>
      <th class="text-left" style="width: 18%">指标</th>
      <th class="text-left" style="width: 18%">用户变量</th>
      <th class="text-left" style="width: 39%">描述</th>
      <th class="text-left" style="width: 10%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1">Operator</th>
      <td>millisBehindLatest</td>
      <td>stream, shardId</td>
      <td>
            对于每个 Kinesis 分片，消费者落后于流头部的毫秒数，指示消费者落后当前时间多远。
            可以通过流名称和分片 ID 指定特定分片的指标。值为 0 表示记录处理被赶上，此时没有新记录要处理。
            值为 -1 表示尚未报告该指标的值。
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>sleepTimeMillis</td>
      <td>stream, shardId</td>
      <td>消费者在从 Kinesis 获取记录之前休眠的毫秒数。可以通过流名称和分片 ID 指定特定分片的指标。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>maxNumberOfRecordsPerFetch</td>
      <td>stream, shardId</td>
      <td>消费者在对 Kinesis 的单个 getRecords 调用中请求的最大记录数。如果 ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS
      设置为 true，则会自适应计算此值用来最大化 Kinesis 的 2 Mbps 读取限制。
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>numberOfAggregatedRecordsPerFetch</td>
      <td>stream, shardId</td>
      <td>消费者在对 Kinesis 的单个 getRecords 调用中提取的聚合 Kinesis 记录数。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>numberOfDeggregatedRecordsPerFetch</td>
      <td>stream, shardId</td>
      <td>消费者在对 Kinesis 的单个 getRecords 调用中提取的解聚 Kinesis 记录数。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>averageRecordSizeBytes</td>
      <td>stream, shardId</td>
      <td>Kinesis 记录的平均大小（以字节为单位），由使用者在单个 getRecords 调用中获取。 </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>runLoopTimeNanos</td>
      <td>stream, shardId</td>
      <td>消费者在运行循环中花费的实际时间，以纳秒为单位。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>loopFrequencyHz</td>
      <td>stream, shardId</td>
      <td>一秒内对 getRecords 的调用次数。</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>bytesRequestedPerFetch</td>
      <td>stream, shardId</td>
      <td>在对 getRecords 的单个调用中请求的字节数 (2 Mbps / loopFrequencyHz)。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

#### HBase 连接器
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">范围</th>
      <th class="text-left" style="width: 18%">指标</th>
      <th class="text-left" style="width: 18%">用户变量</th>
      <th class="text-left" style="width: 39%">描述</th>
      <th class="text-left" style="width: 10%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1">Operator</th>
      <td>lookupCacheHitRate</td>
      <td>n/a</td>
      <td>查找的缓存命中率。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### 系统资源
默认情况下禁用系统资源报告。当 `metrics.system-resource` 启用时，下面列出的其他指标将在 Job- 和 TaskManager 上可用。
系统资源指标会定期更新，并显示配置间隔（ `metrics.system-resource-probing-interval` ）的平均值。

系统资源报告需要在类路径上存在一个可选的依赖项（例如放置在 Flink 的 `lib` 目录中）：

- `com.github.oshi:oshi-core:3.4.0` （根据 EPL 1.0 许可证获得许可）

包括它的传递依赖：

  - `net.java.dev.jna:jna-platform:jar:4.2.2`
  - `net.java.dev.jna:jna:jar:4.2.2`

在这方面的失败将报告为警告消息，例如在启动期间被 `SystemResourcesMetricsInitializer` 记录的 `NoClassDefFoundError` 日志。

#### 系统 CPU

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">范围</th>
      <th class="text-left" style="width: 25%">中缀</th>
      <th class="text-left" style="width: 23%">指标</th>
      <th class="text-left" style="width: 32%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="12"><strong>Job-/TaskManager</strong></th>
      <td rowspan="12">System.CPU</td>
      <td>Usage</td>
      <td>机器上 CPU 使用率的总体百分比。</td>
    </tr>
    <tr>
      <td>Idle</td>
      <td>机器上 CPU 空闲使用率的百分比。</td>
    </tr>
    <tr>
      <td>Sys</td>
      <td>机器上系统 CPU 使用率的百分比。</td>
    </tr>
    <tr>
      <td>User</td>
      <td>机器上用户 CPU 使用率的百分比。</td>
    </tr>
    <tr>
      <td>IOWait</td>
      <td>机器上 IOWait CPU 使用率的百分比。</td>
    </tr>
    <tr>
      <td>Irq</td>
      <td>机器上 Irq CPU 使用率的百分比。</td>
    </tr>
    <tr>
      <td>SoftIrq</td>
      <td>机器上 SoftIrq CPU 使用率的百分比。</td>
    </tr>
    <tr>
      <td>Nice</td>
      <td>机器上 Nice Idle 使用率的百分比。</td>
    </tr>
    <tr>
      <td>Load1min</td>
      <td>最近 1 分钟的平均 CPU 负载。</td>
    </tr>
    <tr>
      <td>Load5min</td>
      <td>最近 5 分钟的平均 CPU 负载。</td>
    </tr>
    <tr>
      <td>Load15min</td>
      <td>最近 15 分钟的平均 CPU 负载。</td>
    </tr>
    <tr>
      <td>UsageCPU*</td>
      <td>每个处理器的 CPU 使用率百分比。</td>
    </tr>
  </tbody>
</table>

#### 系统内存

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">范围</th>
      <th class="text-left" style="width: 25%">中缀</th>
      <th class="text-left" style="width: 23%">指标</th>
      <th class="text-left" style="width: 32%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="4"><strong>Job-/TaskManager</strong></th>
      <td rowspan="2">System.Memory</td>
      <td>Available</td>
      <td>以字节为单位的可用内存。</td>
    </tr>
    <tr>
      <td>Total</td>
      <td>以字节为单位的总内存。</td>
    </tr>
    <tr>
      <td rowspan="2">System.Swap</td>
      <td>Used</td>
      <td>使用的交换内存。</td>
    </tr>
    <tr>
      <td>Total</td>
      <td>总交换内存。</td>
    </tr>
  </tbody>
</table>

#### 系统网络

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">范围</th>
      <th class="text-left" style="width: 25%">中缀</th>
      <th class="text-left" style="width: 23%">指标</th>
      <th class="text-left" style="width: 32%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>Job-/TaskManager</strong></th>
      <td rowspan="2">System.Network.INTERFACE_NAME</td>
      <td>ReceiveRate</td>
      <td>以每秒字节数为单位的平均接收速率。</td>
    </tr>
    <tr>
      <td>SendRate</td>
      <td>以每秒字节数为单位的平均发送速率。</td>
    </tr>
  </tbody>
</table>

## 端到端延迟跟踪

Flink 允许跟踪通过系统的记录的延迟。默认情况下禁用此功能。
要启用延迟跟踪，你必须在 [Flink 配置]({{< ref "docs/deployment/config" >}}#metrics-latency-interval) 或者
 `ExecutionConfig` 中设置 `latencyTrackingInterval` 为正数。

在 `latencyTrackingInterval` 中，源将定期发射一个特殊的记录，称为 `LatencyMarker`。
该标记包含一个时间戳，从记录在源头被发射的时间开始。
延迟标记不能超过普通的用户记录，因此如果记录在操作算子前排队将增加标记所追踪的延迟。

请注意，延迟标记并没有考虑到用户记录在运营商中花费的时间，因为它们是绕过运营商的。特别是这些标记没有考虑到记录在窗口缓冲区等地方
所花费的时间。只有当运营商不能接受新的记录，因此他们在排队，使用标记物测量的延迟才会反映出来。

`LatencyMarker` 被用来得出拓扑结构的源头和每个下游运营商之间的延迟分布。这些分布被报告为直方图指标。
这些分布的颗粒度可以在 [Flink 配置]({{< ref "docs/deployment/config" >}}#metrics-latency-interval)中控制。
对于最高粒度的子任务，Flink 将得出每个源子任务和每个下游子任务之间的延迟分布，这导致直方图的数量呈四次方（以平行度计算）。

目前，Flink 假设集群中所有机器的时钟是同步的。我们建议设置一个自动时钟同步服务（如 NTP），以避免错误的延迟结果。

<span class="label label-danger">警告</span> 启用延迟指标会显著影响集群的性能（特别是子任务的粒度）。强烈建议只将其用于调试目的。

## 状态访问延迟跟踪

Flink 还允许跟踪标准 Flink 状态后端或从 `AbstractStateBackend` 扩展的定制状态后端的键控状态访问延迟。这个功能默认是禁用的。
要启用这个功能你必须在 [Flink 配置]({{< ref "docs/deployment/config" >}}##state-backends-latency-tracking-options)
中把 `state.backend.latency-track.keyed-state-enabled` 设置为true。

一旦跟踪键控状态访问延迟被启用，Flink 将每隔 `N` 次访问对状态访问延迟进行采样，其中 `N` 是由 `state.backend.latency-track.sample-interval` 定义的。
此配置的默认值为100。一个较小的值会得到更准确的结果，但对性能的影响更大，因为它的采样频率更高。

由于这个延迟指标的类型是直方图，`state.backend.latency-track.history-size` 将控制历史记录的最大数量，其默认值为 128。
这个配置的值越大，需要的内存就越多，但会提供一个更准确的结果。

## REST API 集成

可以通过[监控 REST API]({{< ref "docs/ops/rest_api" >}}) 查询指标。

下面是可用端点的列表，以及一个示例 JSON 响应。所有端点都是 `http://hostname:8081/jobmanager/metrics` 形式，下面我们仅列出URL的路径部分。

尖括号中的值是变量，例如 `http://hostname:8081/jobs/<jobid>/metrics` 的实例必须类似
 `http://hostname:8081/jobs/7684be6004e4e955c2a558a9bc463f65/metrics` 。

请求特定实体的指标：

  - `/jobmanager/metrics`
  - `/taskmanagers/<taskmanagerid>/metrics`
  - `/jobs/<jobid>/metrics`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtaskindex>`

跨相应类型的所有实体聚合的请求指标：

  - `/taskmanagers/metrics`
  - `/jobs/metrics`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/metrics`

请求指标在相应类型的所有实体的子集上聚合：

  - `/taskmanagers/metrics?taskmanagers=A,B,C`
  - `/jobs/metrics?jobs=D,E,F`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/metrics?subtask=1,2,3`

<span class="label label-danger">警告</span> 指标名称可以包含查询指标时需要转义的特殊字符。
例如，"`a_+_b`" 将转义为 "`a_%2B_b`"。

应该转义的字符列表：

<table class="table table-bordered">
    <thead>
        <tr>
            <th>字符</th>
            <th>转义列表</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>#</td>
            <td>%23</td>
        </tr>
        <tr>
            <td>$</td>
            <td>%24</td>
        </tr>
        <tr>
            <td>&</td>
            <td>%26</td>
        </tr>
        <tr>
            <td>+</td>
            <td>%2B</td>
        </tr>
        <tr>
            <td>/</td>
            <td>%2F</td>
        </tr>
        <tr>
            <td>;</td>
            <td>%3B</td>
        </tr>
        <tr>
            <td>=</td>
            <td>%3D</td>
        </tr>
        <tr>
            <td>?</td>
            <td>%3F</td>
        </tr>
        <tr>
            <td>@</td>
            <td>%40</td>
        </tr>
    </tbody>
</table>

请求可用指标列表：

`GET /jobmanager/metrics`

```json
[
  {
    "id": "metric1"
  },
  {
    "id": "metric2"
  }
]
```

请求特定（未聚合）指标的值：

`GET taskmanagers/ABCDE/metrics?get=metric1,metric2`

```json
[
  {
    "id": "metric1",
    "value": "34"
  },
  {
    "id": "metric2",
    "value": "2"
  }
]
```

请求特定指标的聚合值：

`GET /taskmanagers/metrics?get=metric1,metric2`

```json
[
  {
    "id": "metric1",
    "min": 1,
    "max": 34,
    "avg": 15,
    "sum": 45
  },
  {
    "id": "metric2",
    "min": 2,
    "max": 14,
    "avg": 7,
    "sum": 16
  }
]
```

请求特定指标的特定聚合值：

`GET /taskmanagers/metrics?get=metric1,metric2&agg=min,max`

```json
[
  {
    "id": "metric1",
    "min": 1,
    "max": 34
  },
  {
    "id": "metric2",
    "min": 2,
    "max": 14
  }
]
```

## 集成仪表板

为每个任务或操作算子收集的指标也可以在仪表板上可视化。在工作的主页上，选择 工作的主页上，选择 `Metrics` 标签。
在顶部图表中选择一个任务后，你可以使用 `Add Metric` 下拉菜单选择要显示的指标。

* 任务指标以 `<subtask_index>.<metric_name>` 的形式列出。
* 操作员指标以 `<subtask_index>.<operator_name>.<metric_name>` 的形式列出。

每个指标将被视为一个单独的图表，X轴代表时间，Y轴代表测量值。
所有的图表都是每10秒自动更新一次，并且在导航到另一个页面时继续更新。

可视化指标的数量没有限制，但是只有数字指标可以被可视化。

{{< top >}}
