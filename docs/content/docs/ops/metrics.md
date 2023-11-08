---
title: "Metrics"
weight: 6
type: docs
aliases:
  - /ops/metrics.html
  - /apis/metrics.html
  - /monitoring/metrics.html
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

# Metrics

Flink exposes a metric system that allows gathering and exposing metrics to external systems.

## Registering metrics

You can access the metric system from any user function that extends [RichFunction]({{< ref "docs/dev/datastream/user_defined_functions" >}}#rich-functions) by calling `getRuntimeContext().getMetricGroup()`.
This method returns a `MetricGroup` object on which you can create and register new metrics.

### Metric types

Flink supports `Counters`, `Gauges`, `Histograms` and `Meters`.

#### Counter

A `Counter` is used to count something. The current value can be in- or decremented using `inc()/inc(long n)` or `dec()/dec(long n)`.
You can create and register a `Counter` by calling `counter(String name)` on a `MetricGroup`.

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
{{< tab "Python" >}}
```python

class MyMapper(MapFunction):
    def __init__(self):
        self.counter = None

    def open(self, runtime_context: RuntimeContext):
        self.counter = runtime_context \
            .get_metrics_group() \
            .counter("my_counter")

    def map(self, value: str):
        self.counter.inc()
        return value
```
{{< /tab >}}
{{< /tabs >}}

Alternatively you can also use your own `Counter` implementation:

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
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

#### Gauge

A `Gauge` provides a value of any type on demand. In order to use a `Gauge` you must first create a class that implements the `org.apache.flink.metrics.Gauge` interface.
There is no restriction for the type of the returned value.
You can register a gauge by calling `gauge(String name, Gauge gauge)` on a `MetricGroup`.

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
{{< tab "Python" >}}
```python

class MyMapper(MapFunction):
    def __init__(self):
        self.value_to_expose = 0

    def open(self, runtime_context: RuntimeContext):
        runtime_context \
            .get_metrics_group() \
            .gauge("my_gauge", lambda: self.value_to_expose)

    def map(self, value: str):
        self.value_to_expose += 1
        return value

```
{{< /tab >}}
{{< /tabs >}}

Note that reporters will turn the exposed object into a `String`, which means that a meaningful `toString()` implementation is required.

#### Histogram

A `Histogram` measures the distribution of long values.
You can register one by calling `histogram(String name, Histogram histogram)` on a `MetricGroup`.

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
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

Flink does not provide a default implementation for `Histogram`, but offers a {{< gh_link file="flink-metrics/flink-metrics-dropwizard/src/main/java/org/apache/flink/dropwizard/metrics/DropwizardHistogramWrapper.java" name="Wrapper" >}} that allows usage of Codahale/DropWizard histograms.
To use this wrapper add the following dependency in your `pom.xml`:
```xml
<dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-metrics-dropwizard</artifactId>
      <version>{{< version >}}</version>
</dependency>
```

You can then register a Codahale/DropWizard histogram like this:

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
    val dropwizardHistogram =
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
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

#### Meter

A `Meter` measures an average throughput. An occurrence of an event can be registered with the `markEvent()` method. Occurrence of multiple events at the same time can be registered with `markEvent(long n)` method.
You can register a meter by calling `meter(String name, Meter meter)` on a `MetricGroup`.

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
{{< tab "Python" >}}
```python

class MyMapperMeter(MapFunction):
    def __init__(self):
        self.meter = None

    def open(self, runtime_context: RuntimeContext):
        # an average rate of events per second over 120s, default is 60s.
        self.meter = runtime_context
            .get_metrics_group()
            .meter("my_meter", time_span_in_seconds=120)

    def map(self, value: str):
        self.meter.mark_event()
        return value

```
{{< /tab >}}
{{< /tabs >}}

Flink offers a {{< gh_link file="flink-metrics/flink-metrics-dropwizard/src/main/java/org/apache/flink/dropwizard/metrics/DropwizardMeterWrapper.java" name="Wrapper" >}} that allows usage of Codahale/DropWizard meters.
To use this wrapper add the following dependency in your `pom.xml`:
```xml
<dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-metrics-dropwizard</artifactId>
      <version>{{< version >}}</version>
</dependency>
```

You can then register a Codahale/DropWizard meter like this:

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
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

## Scope

Every metric is assigned an identifier and a set of key-value pairs under which the metric will be reported.

The identifier is based on 3 components: a user-defined name when registering the metric, an optional user-defined scope and a system-provided scope.
For example, if `A.B` is the system scope, `C.D` the user scope and `E` the name, then the identifier for the metric will be `A.B.C.D.E`.

You can configure which delimiter to use for the identifier (default: `.`) by setting the `metrics.scope.delimiter` key in `conf/flink-conf.yaml`.

### User Scope

You can define a user scope by calling `MetricGroup#addGroup(String name)`, `MetricGroup#addGroup(int name)` or `MetricGroup#addGroup(String key, String value)`.
These methods affect what `MetricGroup#getMetricIdentifier` and `MetricGroup#getScopeComponents` return.

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
{{< tab "Python" >}}
```python

counter = runtime_context \
    .get_metric_group() \
    .add_group("my_metrics") \
    .counter("my_counter")

counter = runtime_context \
    .get_metric_group() \
    .add_group("my_metrics_key", "my_metrics_value") \
    .counter("my_counter")

```
{{< /tab >}}
{{< /tabs >}}

### System Scope

The system scope contains context information about the metric, for example in which task it was registered or what job that task belongs to.

Which context information should be included can be configured by setting the following keys in `conf/flink-conf.yaml`.
Each of these keys expect a format string that may contain constants (e.g. "taskmanager") and variables (e.g. "&lt;task_id&gt;") which will be replaced at runtime.

- `metrics.scope.jm`
  - Default: &lt;host&gt;.jobmanager
  - Applied to all metrics that were scoped to a job manager.
- `metrics.scope.jm-job`
  - Default: &lt;host&gt;.jobmanager.&lt;job_name&gt;
  - Applied to all metrics that were scoped to a job manager and job.
- `metrics.scope.tm`
  - Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;
  - Applied to all metrics that were scoped to a task manager.
- `metrics.scope.tm-job`
  - Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;
  - Applied to all metrics that were scoped to a task manager and job.
- `metrics.scope.task`
  - Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;task_name&gt;.&lt;subtask_index&gt;
   - Applied to all metrics that were scoped to a task.
- `metrics.scope.operator`
  - Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;operator_name&gt;.&lt;subtask_index&gt;
  - Applied to all metrics that were scoped to an operator.

There are no restrictions on the number or order of variables. Variables are case sensitive.

The default scope for operator metrics will result in an identifier akin to `localhost.taskmanager.1234.MyJob.MyOperator.0.MyMetric`

If you also want to include the task name but omit the task manager information you can specify the following format:

`metrics.scope.operator: <host>.<job_name>.<task_name>.<operator_name>.<subtask_index>`

This could create the identifier `localhost.MyJob.MySource_->_MyOperator.MyOperator.0.MyMetric`.

Note that for this format string an identifier clash can occur should the same job be run multiple times concurrently, which can lead to inconsistent metric data.
As such it is advised to either use format strings that provide a certain degree of uniqueness by including IDs (e.g &lt;job_id&gt;)
or by assigning unique names to jobs and operators.

### List of all Variables

- JobManager: &lt;host&gt;
- TaskManager: &lt;host&gt;, &lt;tm_id&gt;
- Job: &lt;job_id&gt;, &lt;job_name&gt;
- Task: &lt;task_id&gt;, &lt;task_name&gt;, &lt;task_attempt_id&gt;, &lt;task_attempt_num&gt;, &lt;subtask_index&gt;
- Operator: &lt;operator_id&gt;,&lt;operator_name&gt;, &lt;subtask_index&gt;

**Important:** For the Batch API, &lt;operator_id&gt; is always equal to &lt;task_id&gt;.

### User Variables

You can define a user variable by calling `MetricGroup#addGroup(String key, String value)`.
This method affects what `MetricGroup#getMetricIdentifier`, `MetricGroup#getScopeComponents` and `MetricGroup#getAllVariables()` returns.

**Important:** User variables cannot be used in scope formats.

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

## Reporter

For information on how to set up Flink's metric reporters please take a look at the [metric reporters documentation]({{< ref "docs/deployment/metric_reporters" >}}).

## System metrics

By default Flink gathers several metrics that provide deep insights on the current state.
This section is a reference of all these metrics.

The tables below generally feature 5 columns:

* The "Scope" column describes which scope format is used to generate the system scope.
  For example, if the cell contains "Operator" then the scope format for "metrics.scope.operator" is used.
  If the cell contains multiple values, separated by a slash, then the metrics are reported multiple
  times for different entities, like for both job- and taskmanagers.

* The (optional)"Infix" column describes which infix is appended to the system scope.

* The "Metrics" column lists the names of all metrics that are registered for the given scope and infix.

* The "Description" column provides information as to what a given metric is measuring.

* The "Type" column describes which metric type is used for the measurement.

Note that all dots in the infix/metric name columns are still subject to the "metrics.delimiter" setting.

Thus, in order to infer the metric identifier:

1. Take the scope-format based on the "Scope" column
2. Append the value in the "Infix" column if present, and account for the "metrics.delimiter" setting
3. Append metric name.

### CPU
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 22%">Infix</th>
      <th class="text-left" style="width: 20%">Metrics</th>
      <th class="text-left" style="width: 32%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>Job-/TaskManager</strong></th>
      <td rowspan="2">Status.JVM.CPU</td>
      <td>Load</td>
      <td>The recent CPU usage of the JVM.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Time</td>
      <td>The CPU time used by the JVM.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### Memory
The memory-related metrics require Oracle's memory management (also included in OpenJDK's Hotspot implementation) to be in place.
Some metrics might not be exposed when using other JVM implementations (e.g. IBM's J9).
<table class="table table-bordered">                               
  <thead>                                                          
    <tr>                                                           
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 22%">Infix</th>          
      <th class="text-left" style="width: 20%">Metrics</th>                           
      <th class="text-left" style="width: 32%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>                       
    </tr>                                                          
  </thead>                                                         
  <tbody>                                                          
    <tr>                                                           
      <th rowspan="17"><strong>Job-/TaskManager</strong></th>
      <td rowspan="15">Status.JVM.Memory</td>
      <td>Heap.Used</td>
      <td>The amount of heap memory currently used (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Heap.Committed</td>
      <td>The amount of heap memory guaranteed to be available to the JVM (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Heap.Max</td>
      <td>The maximum amount of heap memory that can be used for memory management (in bytes). <br/>
      This value might not be necessarily equal to the maximum value specified through -Xmx or
      the equivalent Flink configuration parameter. Some GC algorithms allocate heap memory that won't
      be available to the user code and, therefore, not being exposed through the heap metrics.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>NonHeap.Used</td>
      <td>The amount of non-heap memory currently used (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>NonHeap.Committed</td>
      <td>The amount of non-heap memory guaranteed to be available to the JVM (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>NonHeap.Max</td>
      <td>The maximum amount of non-heap memory that can be used for memory management (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Metaspace.Used</td>
      <td>The amount of memory currently used in the Metaspace memory pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Metaspace.Committed</td>
      <td>The amount of memory guaranteed to be available to the JVM in the Metaspace memory pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Metaspace.Max</td>
      <td>The maximum amount of memory that can be used in the Metaspace memory pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Direct.Count</td>
      <td>The number of buffers in the direct buffer pool.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Direct.MemoryUsed</td>
      <td>The amount of memory used by the JVM for the direct buffer pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Direct.TotalCapacity</td>
      <td>The total capacity of all buffers in the direct buffer pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Mapped.Count</td>
      <td>The number of buffers in the mapped buffer pool.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Mapped.MemoryUsed</td>
      <td>The amount of memory used by the JVM for the mapped buffer pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Mapped.TotalCapacity</td>
      <td>The number of buffers in the mapped buffer pool (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="2">Status.Flink.Memory</td>
      <td>Managed.Used</td>
      <td>The amount of managed memory currently used.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Managed.Total</td>
      <td>The total amount of managed memory.</td>
      <td>Gauge</td>
    </tr>
  </tbody>                                                         
</table>

### Threads
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 22%">Infix</th>
      <th class="text-left" style="width: 20%">Metrics</th>
      <th class="text-left" style="width: 32%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1"><strong>Job-/TaskManager</strong></th>
      <td rowspan="1">Status.JVM.Threads</td>
      <td>Count</td>
      <td>The total number of live threads.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### GarbageCollection
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 22%">Infix</th>
      <th class="text-left" style="width: 20%">Metrics</th>
      <th class="text-left" style="width: 32%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="3"><strong>Job-/TaskManager</strong></th>
      <td rowspan="3">Status.JVM.GarbageCollector</td>
      <td>&lt;Collector/All&gt;.Count</td>
      <td>The total number of collections that have occurred for the given (or all) collector.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>&lt;Collector/All&gt;.Time</td>
      <td>The total time spent performing garbage collection for the given (or all) collector.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>&lt;Collector/All&gt;.TimeMsPerSecond</td>
      <td>The time (in milliseconds) spent garbage collecting per second for the given (or all) collector.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### ClassLoader
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 22%">Infix</th>
      <th class="text-left" style="width: 20%">Metrics</th>
      <th class="text-left" style="width: 32%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>Job-/TaskManager</strong></th>
      <td rowspan="2">Status.JVM.ClassLoader</td>
      <td>ClassesLoaded</td>
      <td>The total number of classes loaded since the start of the JVM.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>ClassesUnloaded</td>
      <td>The total number of classes unloaded since the start of the JVM.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>


### Network

{{< hint warning >}}
Deprecated: use [Default shuffle service metrics](#default-shuffle-service)
{{< /hint >}}

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 22%">Infix</th>
      <th class="text-left" style="width: 22%">Metrics</th>
      <th class="text-left" style="width: 30%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>TaskManager</strong></th>
      <td rowspan="2">Status.Network</td>
      <td>AvailableMemorySegments</td>
      <td>The number of unused memory segments.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>TotalMemorySegments</td>
      <td>The number of allocated memory segments.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="10">Task</th>
      <td rowspan="6">buffers</td>
      <td>inputQueueLength</td>
      <td>The number of queued input buffers. (ignores LocalInputChannels which are using blocking subpartitions)</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>outputQueueLength</td>
      <td>The number of queued output buffers.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inPoolUsage</td>
      <td>An estimate of the input buffers usage. (ignores LocalInputChannels)</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inputFloatingBuffersUsage</td>
      <td>An estimate of the floating input buffers usage. (ignores LocalInputChannels)</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inputExclusiveBuffersUsage</td>
      <td>An estimate of the exclusive input buffers usage. (ignores LocalInputChannels)</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>outPoolUsage</td>
      <td>An estimate of the output buffers usage. The pool usage can be > 100% if overdraft buffers are being used.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="4">Network.&lt;Input|Output&gt;.&lt;gate|partition&gt;<br />
        <strong>(only available if <tt>taskmanager.network.detailed-metrics</tt> config option is set)</strong></td>
      <td>totalQueueLen</td>
      <td>Total number of queued buffers in all input/output channels.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>minQueueLen</td>
      <td>Minimum number of queued buffers in all input/output channels.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>maxQueueLen</td>
      <td>Maximum number of queued buffers in all input/output channels.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>avgQueueLen</td>
      <td>Average number of queued buffers in all input/output channels.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### Default shuffle service

Metrics related to data exchange between task executors using netty network communication.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 22%">Infix</th>
      <th class="text-left" style="width: 22%">Metrics</th>
      <th class="text-left" style="width: 30%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="7"><strong>TaskManager</strong></th>
      <td rowspan="7">Status.Shuffle.Netty</td>
      <td>AvailableMemorySegments</td>
      <td>The number of unused memory segments.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>UsedMemorySegments</td>
      <td>The number of used memory segments.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>TotalMemorySegments</td>
      <td>The number of allocated memory segments.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>AvailableMemory</td>
      <td>The amount of unused memory in bytes.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>UsedMemory</td>
      <td>The amount of used memory in bytes.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>TotalMemory</td>
      <td>The amount of allocated memory in bytes.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>RequestedMemoryUsage</td>
      <td>Experimental: The usage of the network memory. Shows (as percentage) the total amount of requested memory from all of the subtasks. It can exceed 100% as not all requested memory is required for subtask to make progress. However if usage exceeds 100% throughput can suffer greatly and please consider increasing available network memory, or decreasing configured size of network buffer pools.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="20">Task</th>
      <td rowspan="5">Shuffle.Netty.Input.Buffers</td>
      <td>inputQueueLength</td>
      <td>The number of queued input buffers.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inputQueueSize</td>
      <td>The real size of queued input buffers in bytes. The size for local input channels is always `0` since the local channel takes records directly from the output queue.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inPoolUsage</td>
      <td>An estimate of the input buffers usage. (ignores LocalInputChannels)</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inputFloatingBuffersUsage</td>
      <td>An estimate of the floating input buffers usage. (ignores LocalInputChannels)</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inputExclusiveBuffersUsage</td>
      <td>An estimate of the exclusive input buffers usage. (ignores LocalInputChannels)</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="3">Shuffle.Netty.Output.Buffers</td>
      <td>outputQueueLength</td>
      <td>The number of queued output buffers.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>outputQueueSize</td>
      <td>The real size of queued output buffers in bytes. </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>outPoolUsage</td>
      <td>An estimate of the output buffers usage. The pool usage can be > 100% if overdraft buffers are being used.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="4">Shuffle.Netty.&lt;Input|Output&gt;.&lt;gate|partition&gt;<br />
        <strong>(only available if <tt>taskmanager.network.detailed-metrics</tt> config option is set)</strong></td>
      <td>totalQueueLen</td>
      <td>Total number of queued buffers in all input/output channels.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>minQueueLen</td>
      <td>Minimum number of queued buffers in all input/output channels.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>maxQueueLen</td>
      <td>Maximum number of queued buffers in all input/output channels.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>avgQueueLen</td>
      <td>Average number of queued buffers in all input/output channels.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="8">Shuffle.Netty.Input</td>
      <td>numBytesInLocal</td>
      <td>The total number of bytes this task has read from a local source.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesInLocalPerSecond</td>
      <td>The number of bytes this task reads from a local source per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBytesInRemote</td>
      <td>The total number of bytes this task has read from a remote source.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesInRemotePerSecond</td>
      <td>The number of bytes this task reads from a remote source per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersInLocal</td>
      <td>The total number of network buffers this task has read from a local source.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersInLocalPerSecond</td>
      <td>The number of network buffers this task reads from a local source per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersInRemote</td>
      <td>The total number of network buffers this task has read from a remote source.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersInRemotePerSecond</td>
      <td>The number of network buffers this task reads from a remote source per second.</td>
      <td>Meter</td>
    </tr>
  </tbody>
</table>

### Cluster
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 26%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5"><strong>JobManager</strong></th>
      <td>numRegisteredTaskManagers</td>
      <td>The number of registered taskmanagers.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numPendingTaskManagers</td>
      <td>(only applicable to Native Kubernetes / YARN) The number of outstanding taskmanagers that Flink has requested.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numRunningJobs</td>
      <td>The number of running jobs.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>taskSlotsAvailable</td>
      <td>The number of available task slots.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>taskSlotsTotal</td>
      <td>The total number of task slots.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### Availability

The metrics in this table are available for each of the following job states: INITIALIZING, CREATED, RUNNING, RESTARTING, CANCELLING, FAILING.
Whether these metrics are reported depends on the [metrics.job.status.enable]({{< ref "docs/deployment/config" >}}#metrics-job-status-enable) setting.

<span class="label label-info">Evolving</span> The semantics of these metrics may change in later releases.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 26%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="3"><strong>Job (only available on JobManager)</strong></th>
      <td>&lt;jobStatus&gt;State</td>
      <td>For a given state, return 1 if the job is currently in that state, otherwise return 0.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>&lt;jobStatus&gt;Time</td>
      <td>For a given state, if the job is currently in that state, return the time (in milliseconds) since the job transitioned into that state, otherwise return 0.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>&lt;jobStatus&gt;TimeTotal</td>
      <td>For a given state, return how much time (in milliseconds) the job has spent in that state in total.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

{{< hint info >}}
<span class="label label-info">Experimental</span>

While the job is in the RUNNING state the metrics in this table provide additional details on what the job is currently doing.
Whether these metrics are reported depends on the [metrics.job.status.enable]({{< ref "docs/deployment/config" >}}#metrics-job-status-enable) setting.

<table class="table table-bordered table-inline">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 26%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="3"><strong>Job (only available on JobManager)</strong></th>
      <td>deployingState</td>
      <td>Return 1 if the job is currently deploying* tasks, otherwise return 0.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>deployingTime</td>
      <td>Return the time (in milliseconds) since the job has started deploying* tasks, otherwise return 0.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>deployingTimeTotal</td>
      <td>Return how much time (in milliseconds) the job has spent deploying* tasks in total.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

*A job is considered to be deploying tasks when:
* for streaming jobs, any task is in the DEPLOYING state
* for batch jobs, if at least 1 task is in the DEPLOYING state, and there are no INITIALIZING/RUNNING tasks
{{< /hint >}}

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 26%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="4"><strong>Job (only available on JobManager)</strong></th>
      <td>uptime</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <b>runningTime</b>.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>downtime</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <b>restartingTime</b>, <b>cancellingTime</b> <b>failingTime</b>.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>fullRestarts</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <b>numRestarts</b>.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numRestarts</td>
      <td>The total number of restarts since this job was submitted, including full restarts and fine-grained restarts.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### Checkpointing

Note that for failed checkpoints, metrics are updated on a best efforts basis and may be not accurate.
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 26%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="10"><strong>Job (only available on JobManager)</strong></th>
      <td>lastCheckpointDuration</td>
      <td>The time it took to complete the last checkpoint (in milliseconds).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastCheckpointSize</td>
      <td>The checkpointed size of the last checkpoint (in bytes), this metric could be different from lastCheckpointFullSize if incremental checkpoint or changelog is enabled.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastCompletedCheckpointId</td>
      <td>The identifier of the last completed checkpoint.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastCheckpointFullSize</td>
      <td>The full size of the last checkpoint (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastCheckpointExternalPath</td>
      <td>The path where the last external checkpoint was stored.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastCheckpointRestoreTimestamp</td>
      <td>Timestamp when the last checkpoint was restored at the coordinator (in milliseconds).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numberOfInProgressCheckpoints</td>
      <td>The number of in progress checkpoints.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numberOfCompletedCheckpoints</td>
      <td>The number of successfully completed checkpoints.</td>
      <td>Gauge</td>
    </tr>            
    <tr>
      <td>numberOfFailedCheckpoints</td>
      <td>The number of failed checkpoints.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>totalNumberOfCheckpoints</td>
      <td>The number of total checkpoints (in progress, completed, failed).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="2"><strong>Task</strong></th>
      <td>checkpointAlignmentTime</td>
      <td>The time in nanoseconds that the last barrier alignment took to complete, or how long the current alignment has taken so far (in nanoseconds). This is the time between receiving first and the last checkpoint barrier. You can find more information in the [Monitoring State and Checkpoints section]({{< ref "docs/ops/state/large_state_tuning" >}}#monitoring-state-and-checkpoints)</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>checkpointStartDelayNanos</td>
      <td>The time in nanoseconds that elapsed between the creation of the last checkpoint and the time when the checkpointing process has started by this Task. This delay shows how long it takes for the first checkpoint barrier to reach the task. A high value indicates back-pressure. If only a specific task has a long start delay, the most likely reason is data skew.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### State Access Latency

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 26%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="27"><strong>Task/Operator</strong></th>
      <td>stateClearLatency</td>
      <td>The latency of clear operation for state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>valueStateGetLatency</td>
      <td>The latency of Get operation for value state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>valueStateUpdateLatency</td>
      <td>The latency of update operation for value state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>listStateGetLatency</td>
      <td>The latency of get operation for list state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>listStateAddLatency</td>
      <td>The latency of add operation for list state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>listStateAddAllLatency</td>
      <td>The latency of addAll operation for list state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>listStateUpdateLatency</td>
      <td>The latency of update operation for list state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>listStateMergeNamespacesLatency</td>
      <td>The latency of merge namespace operation for list state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateGetLatency</td>
      <td>The latency of get operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStatePutLatency</td>
      <td>The latency of put operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStatePutAllLatency</td>
      <td>The latency of putAll operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateRemoveLatency</td>
      <td>The latency of remove operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateContainsLatency</td>
      <td>The latency of contains operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateEntriesInitLatency</td>
      <td>The init latency of entries operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateKeysInitLatency</td>
      <td>The init latency of keys operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateValuesInitLatency</td>
      <td>The init latency of values operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateIteratorInitLatency</td>
      <td>The init latency of iterator operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateIsEmptyLatency</td>
      <td>The latency of isEmpty operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateIteratorHasNextLatency</td>
      <td>The latency of iterator#hasNext operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateIteratorNextLatency</td>
      <td>The latency of iterator#next operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mapStateIteratorRemoveLatency</td>
      <td>The latency of iterator#remove operation for map state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>aggregatingStateGetLatency</td>
      <td>The latency of get operation for aggregating state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>aggregatingStateAddLatency</td>
      <td>The latency of add operation for aggregating state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>aggregatingStateMergeNamespacesLatency</td>
      <td>The latency of merge namespace operation for aggregating state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>reducingStateGetLatency</td>
      <td>The latency of get operation for reducing state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>reducingStateAddLatency</td>
      <td>The latency of add operation for reducing state</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>reducingStateMergeNamespacesLatency</td>
      <td>The latency of merge namespace operation for reducing state</td>
      <td>Histogram</td>
    </tr>
  </tbody>
</table>

### RocksDB
Certain RocksDB native metrics are available but disabled by default, you can find full documentation [here]({{< ref "docs/deployment/config" >}}#rocksdb-native-metrics)

### State Changelog

Note that the metrics are only available via reporters.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 26%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="8"><strong>Job (only available on TaskManager)</strong></th>
      <td>numberOfUploadRequests</td>
      <td>Total number of upload requests made</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numberOfUploadFailures</td>
      <td>Total number of failed upload requests (request may be retried after the failure)</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>attemptsPerUpload</td>
      <td>The number of attempts per upload</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>totalAttemptsPerUpload</td>
      <td>The total count distributions of attempts for per upload</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>uploadBatchSizes</td>
      <td>The number of upload tasks (coming from one or more writers, i.e. backends/tasks) that were grouped together and form a single upload resulting in a single file</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>uploadLatenciesNanos</td>
      <td>The latency distributions of uploads</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>uploadSizes</td>
      <td>The size distributions of uploads</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>uploadQueueSize</td>
      <td>Current size of upload queue. Queue items can be packed together and form a single upload.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="8"><strong>Task/Operator</strong></th>
      <td>startedMaterialization</td>
      <td>The number of started materializations.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>completedMaterialization</td>
      <td>The number of successfully completed materializations.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>failedMaterialization</td>
      <td>The number of failed materializations.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>lastDurationOfMaterialization</td>
      <td>The duration of the last materialization (in milliseconds).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastFullSizeOfMaterialization</td>
      <td>The full size of the materialization part of the last reported checkpoint (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastIncSizeOfMaterialization</td>
      <td>The incremental size of the materialization part of the last reported checkpoint (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastFullSizeOfNonMaterialization</td>
      <td>The full size of the non-materialization part of the last reported checkpoint (in bytes).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastIncSizeOfNonMaterialization</td>
      <td>The incremental size of the non-materialization part of the last reported checkpoint (in bytes).</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### IO
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 26%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1"><strong>Job (only available on TaskManager)</strong></th>
      <td>[&lt;source_id&gt;.[&lt;source_subtask_index&gt;.]]&lt;operator_id&gt;.&lt;operator_subtask_index&gt;.latency</td>
      <td>The latency distributions from a given source (subtask) to an operator subtask (in milliseconds), depending on the <a href="{{< ref "docs/deployment/config" >}}#metrics-latency-granularity">latency granularity</a>.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <th rowspan="25"><strong>Task</strong></th>
      <td>numBytesInLocal</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{< ref "docs/ops/metrics" >}}#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesInLocalPerSecond</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{< ref "docs/ops/metrics" >}}#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBytesInRemote</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{< ref "docs/ops/metrics" >}}#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesInRemotePerSecond</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{< ref "docs/ops/metrics" >}}#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersInLocal</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{< ref "docs/ops/metrics" >}}#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersInLocalPerSecond</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{< ref "docs/ops/metrics" >}}#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersInRemote</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{< ref "docs/ops/metrics" >}}#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersInRemotePerSecond</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{< ref "docs/ops/metrics" >}}#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBytesOut</td>
      <td>The total number of bytes this task has emitted.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesOutPerSecond</td>
      <td>The number of bytes this task emits per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersOut</td>
      <td>The total number of network buffers this task has emitted.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersOutPerSecond</td>
      <td>The number of network buffers this task emits per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>isBackPressured</td>
      <td>Whether the task is back-pressured.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>idleTimeMsPerSecond</td>
      <td>The time (in milliseconds) this task is idle (has no data to process) per second. Idle time excludes back pressured time, so if the task is back pressured it is not idle.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>busyTimeMsPerSecond</td>
      <td>The time (in milliseconds) this task is busy (neither idle nor back pressured) per second. Can be NaN, if the value could not be calculated.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>backPressuredTimeMsPerSecond</td>
      <td>The time (in milliseconds) this task is back pressured (soft or hard) per second. It's a sum of softBackPressuredTimeMsPerSecond and hardBackPressuredTimeMsPerSecond.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>softBackPressuredTimeMsPerSecond</td>
      <td>The time (in milliseconds) this task is softly back pressured per second. Softly back pressured task will be still responsive and capable of for example triggering unaligned checkpoints.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>hardBackPressuredTimeMsPerSecond</td>
      <td>The time (in milliseconds) this task is back pressured in a hard way per second. During hard back pressured task is completely blocked and unresponsive preventing for example unaligned checkpoints from triggering.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>maxSoftBackPressuredTimeMs</td>
      <td>Maximum recorded duration of a single consecutive period of the task being softly back pressured in the last sampling period. Please check softBackPressuredTimeMsPerSecond and hardBackPressuredTimeMsPerSecond for more information.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>maxHardBackPressuredTimeMs</td>
      <td>Maximum recorded duration of a single consecutive period of the task being in the hard back pressure state in the last sampling period. Please check softBackPressuredTimeMsPerSecond and hardBackPressuredTimeMsPerSecond for more information.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>changelogBusyTimeMsPerSecond</td>
      <td>The time (in milliseconds) taken by the Changelog state backend to do IO operations, only positive when Changelog state backend is enabled. Please check 'dstl.dfs.upload.max-in-flight' for more information.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>mailboxMailsPerSecond</td>
      <td>The number of actions processed from the task's mailbox per second which includes all actions, e.g., checkpointing, timer, or cancellation actions.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>mailboxLatencyMs</td>
      <td>The latency is the time that actions spend waiting in the task's mailbox before being processed. The metric is a statistic of the latency in milliseconds that is measured approximately once every second and includes the last 60 measurements.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>mailboxQueueSize</td>
      <td>The number of actions in the task's mailbox that are waiting to be processed.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>initializationTime</td>
      <td>The time in milliseconds that one task spends on initialization, return 0 when the task is not in initialization/running status. Most of the initialization time is usually spent in restoring from the checkpoint.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td rowspan="2"><strong>Task (only if buffer debloating is enabled and in non-source tasks)</strong></td>
      <td>estimatedTimeToConsumeBuffersMs</td>
      <td>The estimated time (in milliseconds) by the buffer debloater to consume all of the buffered data in the network exchange preceding this task. This value is calculated by approximated amount of the in-flight data and calculated throughput.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>debloatedBufferSize</td>
      <td>The desired buffer size (in bytes) calculated by the buffer debloater. Buffer debloater is trying to reduce buffer size when the amount of in-flight data (after taking into account current throughput) exceeds the configured target value.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="6"><strong>Task/Operator</strong></th>
      <td>numRecordsIn</td>
      <td>The total number of records this operator/task has received.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numRecordsInPerSecond</td>
      <td>The number of records this operator/task receives per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numRecordsOut</td>
      <td>The total number of records this operator/task has emitted.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numRecordsOutPerSecond</td>
      <td>The number of records this operator/task sends per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numLateRecordsDropped</td>
      <td>The number of records this operator/task has dropped due to arriving late.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>currentInputWatermark</td>
      <td>
        The last watermark this operator/tasks has received (in milliseconds).
        <p><strong>Note:</strong> For operators/tasks with 2 inputs this is the minimum of the last received watermarks.</p>
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="4"><strong>Operator</strong></th>
      <td>currentInput<strong>N</strong>Watermark</td>
      <td>
        The last watermark this operator has received in its <strong>N'th</strong> input (in milliseconds), with index <strong>N</strong> starting from 1. For example currentInput<strong>1</strong>Watermark, currentInput<strong>2</strong>Watermark, ...
        <p><strong>Note:</strong> Only for operators with 2 or more inputs.</p>
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>currentOutputWatermark</td>
      <td>
        The last watermark this operator has emitted (in milliseconds).
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>watermarkAlignmentDrift</td>
      <td>
        The current drift from the minimal watermark emitted by all sources/tasks/splits that belong
        to the same watermark group.
        <p><strong>Note:</strong> Available only when watermark alignment is enabled and the first common watermark is
        announced. You can configure the update interval in the WatermarkStrategy.</p>
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numSplitsProcessed</td>
      <td>The total number of InputSplits this data source has processed (if the operator is a data source).</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### Connectors

#### Kafka Connectors
Please refer to [Kafka monitoring]({{< ref "docs/connectors/datastream/kafka" >}}/#monitoring).

#### Kinesis Source
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">Scope</th>
      <th class="text-left" style="width: 18%">Metrics</th>
      <th class="text-left" style="width: 18%">User Variables</th>
      <th class="text-left" style="width: 39%">Description</th>
      <th class="text-left" style="width: 10%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1">Operator</th>
      <td>millisBehindLatest</td>
      <td>stream, shardId</td>
      <td>The number of milliseconds the consumer is behind the head of the stream,
      indicating how far behind current time the consumer is, for each Kinesis shard.
      A particular shard's metric can be specified by stream name and shard id.
      A value of 0 indicates record processing is caught up, and there are no new records
      to process at this moment. A value of -1 indicates that there is no reported value for the metric, yet.
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>sleepTimeMillis</td>
      <td>stream, shardId</td>
      <td>The number of milliseconds the consumer spends sleeping before fetching records from Kinesis.
      A particular shard's metric can be specified by stream name and shard id.
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>maxNumberOfRecordsPerFetch</td>
      <td>stream, shardId</td>
      <td>The maximum number of records requested by the consumer in a single getRecords call to Kinesis. If ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS
      is set to true, this value is adaptively calculated to maximize the 2 Mbps read limits from Kinesis.
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>numberOfAggregatedRecordsPerFetch</td>
      <td>stream, shardId</td>
      <td>The number of aggregated Kinesis records fetched by the consumer in a single getRecords call to Kinesis.
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>numberOfDeggregatedRecordsPerFetch</td>
      <td>stream, shardId</td>
      <td>The number of deaggregated Kinesis records fetched by the consumer in a single getRecords call to Kinesis.
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>averageRecordSizeBytes</td>
      <td>stream, shardId</td>
      <td>The average size of a Kinesis record in bytes, fetched by the consumer in a single getRecords call.
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>runLoopTimeNanos</td>
      <td>stream, shardId</td>
      <td>The actual time taken, in nanoseconds, by the consumer in the run loop.
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>loopFrequencyHz</td>
      <td>stream, shardId</td>
      <td>The number of calls to getRecords in one second.
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>bytesRequestedPerFetch</td>
      <td>stream, shardId</td>
      <td>The bytes requested (2 Mbps / loopFrequencyHz) in a single call to getRecords.
      </td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

#### Kinesis Sink
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">Scope</th>
      <th class="text-left" style="width: 18%">Metrics</th>
      <th class="text-left" style="width: 39%">Description</th>
      <th class="text-left" style="width: 10%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1">Operator</th>
      <td>numRecordsOutErrors (deprecated, please use numRecordsSendErrors)</td>
      <td>Number of rejected record writes.</td>
      <td>Counter</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <th rowspan="1">Operator</th>
      <td>numRecordsSendErrors</td>
      <td>Number of rejected record writes.</td>
      <td>Counter</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <th rowspan="1">Operator</th>
      <td>CurrentSendTime</td>
      <td>Number of ms taken for 1 round trip of the last request batch.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

#### HBase Connectors
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">Scope</th>
      <th class="text-left" style="width: 18%">Metrics</th>
      <th class="text-left" style="width: 18%">User Variables</th>
      <th class="text-left" style="width: 39%">Description</th>
      <th class="text-left" style="width: 10%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="1">Operator</th>
      <td>lookupCacheHitRate</td>
      <td>n/a</td>
      <td>Cache hit ratio for lookup.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### System resources

System resources reporting is disabled by default. When `metrics.system-resource`
is enabled additional metrics listed below will be available on Job- and TaskManager.
System resources metrics are updated periodically and they present average values for a
configured interval (`metrics.system-resource-probing-interval`).

System resources reporting requires an optional dependency to be present on the
classpath (for example placed in Flink's `lib` directory):

  - `com.github.oshi:oshi-core:6.1.5` (licensed under MIT license)

Including it's transitive dependencies:

  - `net.java.dev.jna:jna-platform:jar:5.10.0`
  - `net.java.dev.jna:jna:jar:5.10.0`

Failures in this regard will be reported as warning messages like `NoClassDefFoundError`
logged by `SystemResourcesMetricsInitializer` during the startup.

#### System CPU

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Scope</th>
      <th class="text-left" style="width: 25%">Infix</th>
      <th class="text-left" style="width: 23%">Metrics</th>
      <th class="text-left" style="width: 32%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="13"><strong>Job-/TaskManager</strong></th>
      <td rowspan="13">System.CPU</td>
      <td>Usage</td>
      <td>Overall % of CPU usage on the machine.</td>
    </tr>
    <tr>
      <td>Idle</td>
      <td>% of CPU Idle time on the machine.</td>
    </tr>
    <tr>
      <td>Sys</td>
      <td>% of System CPU time on the machine.</td>
    </tr>
    <tr>
      <td>User</td>
      <td>% of User CPU time on the machine.</td>
    </tr>
    <tr>
      <td>IOWait</td>
      <td>% of IOWait CPU time on the machine.</td>
    </tr>
    <tr>
      <td>Irq</td>
      <td>% of Irq CPU time on the machine.</td>
    </tr>
    <tr>
      <td>SoftIrq</td>
      <td>% of SoftIrq CPU time on the machine.</td>
    </tr>
    <tr>
      <td>Nice</td>
      <td>% of Nice CPU time on the machine.</td>
    </tr>
    <tr>
      <td>Steal</td>
      <td>% of Steal CPU time on the machine.</td>
    </tr>
    <tr>
      <td>Load1min</td>
      <td>Average CPU load over 1 minute</td>
    </tr>
    <tr>
      <td>Load5min</td>
      <td>Average CPU load over 5 minute</td>
    </tr>
    <tr>
      <td>Load15min</td>
      <td>Average CPU load over 15 minute</td>
    </tr>
    <tr>
      <td>UsageCPU*</td>
      <td>% of CPU usage per each processor</td>
    </tr>
  </tbody>
</table>

#### System memory

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Scope</th>
      <th class="text-left" style="width: 25%">Infix</th>
      <th class="text-left" style="width: 23%">Metrics</th>
      <th class="text-left" style="width: 32%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="4"><strong>Job-/TaskManager</strong></th>
      <td rowspan="2">System.Memory</td>
      <td>Available</td>
      <td>Available memory in bytes</td>
    </tr>
    <tr>
      <td>Total</td>
      <td>Total memory in bytes</td>
    </tr>
    <tr>
      <td rowspan="2">System.Swap</td>
      <td>Used</td>
      <td>Used swap bytes</td>
    </tr>
    <tr>
      <td>Total</td>
      <td>Total swap in bytes</td>
    </tr>
  </tbody>
</table>

#### System network

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Scope</th>
      <th class="text-left" style="width: 25%">Infix</th>
      <th class="text-left" style="width: 23%">Metrics</th>
      <th class="text-left" style="width: 32%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>Job-/TaskManager</strong></th>
      <td rowspan="2">System.Network.INTERFACE_NAME</td>
      <td>ReceiveRate</td>
      <td>Average receive rate in bytes per second</td>
    </tr>
    <tr>
      <td>SendRate</td>
      <td>Average send rate in bytes per second</td>
    </tr>
  </tbody>
</table>

### Speculative Execution

Metrics below can be used to measure the effectiveness of speculative execution.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 26%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2"><strong>Job (only available on JobManager)</strong></th>
      <td>numSlowExecutionVertices</td>
      <td>Number of slow execution vertices at the moment.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>numEffectiveSpeculativeExecutions</td>
      <td>Number of effective speculative execution attempts, i.e. speculative execution attempts which
      finish earlier than their corresponding original attempts.</td>
      <td>Counter</td>
    </tr>
  </tbody>
</table>

## End-to-End latency tracking

Flink allows to track the latency of records travelling through the system. This feature is disabled by default.
To enable the latency tracking you must set the `latencyTrackingInterval` to a positive number in either the
[Flink configuration]({{< ref "docs/deployment/config" >}}#metrics-latency-interval) or `ExecutionConfig`.

At the `latencyTrackingInterval`, the sources will periodically emit a special record, called a `LatencyMarker`.
The marker contains a timestamp from the time when the record has been emitted at the sources.
Latency markers can not overtake regular user records, thus if records are queuing up in front of an operator,
it will add to the latency tracked by the marker.

Note that the latency markers are not accounting for the time user records spend in operators as they are
bypassing them. In particular the markers are not accounting for the time records spend for example in window buffers.
Only if operators are not able to accept new records, thus they are queuing up, the latency measured using
the markers will reflect that.

The `LatencyMarker`s are used to derive a distribution of the latency between the sources of the topology and each
downstream operator. These distributions are reported as histogram metrics. The granularity of these distributions can
be controlled in the [Flink configuration]({{< ref "docs/deployment/config" >}}#metrics-latency-interval). For the highest
granularity `subtask` Flink will derive the latency distribution between every source subtask and every downstream
subtask, which results in quadratic (in the terms of the parallelism) number of histograms.

Currently, Flink assumes that the clocks of all machines in the cluster are in sync. We recommend setting
up an automated clock synchronisation service (like NTP) to avoid false latency results.

<span class="label label-danger">Warning</span> Enabling latency metrics can significantly impact the performance
of the cluster (in particular for `subtask` granularity). It is highly recommended to only use them for debugging
purposes.

## State access latency tracking

Flink also allows to track the keyed state access latency for standard Flink state-backends or customized state backends which extending from `AbstractStateBackend`. This feature is disabled by default.
To enable this feature you must set the `state.backend.latency-track.keyed-state-enabled` to true in the [Flink configuration]({{< ref "docs/deployment/config" >}}#state-backends-latency-tracking-options).

Once tracking keyed state access latency is enabled, Flink will sample the state access latency every `N` access, in which `N` is defined by `state.backend.latency-track.sample-interval`.
This configuration has a default value of 100. A smaller value will get more accurate results but have a higher performance impact since it is sampled more frequently.

As the type of this latency metrics is histogram, `state.backend.latency-track.history-size` will control the maximum number of recorded values in history, which has the default value of 128.
A larger value of this configuration will require more memory, but will provide a more accurate result.

<span class="label label-danger">Warning</span> Enabling state-access-latency metrics may impact the performance.
It is recommended to only use them for debugging purposes.

## REST API integration

Metrics can be queried through the [Monitoring REST API]({{< ref "docs/ops/rest_api" >}}).

Below is a list of available endpoints, with a sample JSON response. All endpoints are of the sample form `http://hostname:8081/jobmanager/metrics`, below we list only the *path* part of the URLs.

Values in angle brackets are variables, for example `http://hostname:8081/jobs/<jobid>/metrics` will have to be requested for example as `http://hostname:8081/jobs/7684be6004e4e955c2a558a9bc463f65/metrics`.

Request metrics for a specific entity:

  - `/jobmanager/metrics`
  - `/taskmanagers/<taskmanagerid>/metrics`
  - `/jobs/<jobid>/metrics`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtaskindex>`

Request metrics aggregated across all entities of the respective type:

  - `/taskmanagers/metrics`
  - `/jobs/metrics`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/metrics`

Request metrics aggregated over a subset of all entities of the respective type:

  - `/taskmanagers/metrics?taskmanagers=A,B,C`
  - `/jobs/metrics?jobs=D,E,F`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/metrics?subtask=1,2,3`

<span class="label label-danger">Warning</span> Metric names can contain special characters that you need to escape when querying metrics.
For example, "`a_+_b`" would be escaped to "`a_%2B_b`".

List of characters that should be escaped:
<table class="table table-bordered">
    <thead>
        <tr>
            <th>Character</th>
            <th>Escape Sequence</th>
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

Request a list of available metrics:

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

Request the values for specific (unaggregated) metrics:

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

Request aggregated values for specific metrics:

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

Request specific aggregated values for specific metrics:

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

## Dashboard integration

Metrics that were gathered for each task or operator can also be visualized in the Dashboard. On the main page for a
job, select the `Metrics` tab. After selecting one of the tasks in the top graph you can select metrics to display using
the `Add Metric` drop-down menu.

* Task metrics are listed as `<subtask_index>.<metric_name>`.
* Operator metrics are listed as `<subtask_index>.<operator_name>.<metric_name>`.

Each metric will be visualized as a separate graph, with the x-axis representing time and the y-axis the measured value.
All graphs are automatically updated every 10 seconds, and continue to do so when navigating to another page.

There is no limit as to the number of visualized metrics; however only numeric metrics can be visualized.

{{< top >}}
