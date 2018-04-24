---
title: "Metrics"
nav-parent_id: monitoring
nav-pos: 1
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

Flink exposes a metric system that allows gathering and exposing metrics to external systems.

* This will be replaced by the TOC
{:toc}

## Registering metrics

You can access the metric system from any user function that extends [RichFunction]({{ site.baseurl }}/dev/api_concepts.html#rich-functions) by calling `getRuntimeContext().getMetricGroup()`.
This method returns a `MetricGroup` object on which you can create and register new metrics.

### Metric types

Flink supports `Counters`, `Gauges`, `Histograms` and `Meters`.

#### Counter

A `Counter` is used to count something. The current value can be in- or decremented using `inc()/inc(long n)` or `dec()/dec(long n)`.
You can create and register a `Counter` by calling `counter(String name)` on a `MetricGroup`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

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

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

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

{% endhighlight %}
</div>

</div>

Alternatively you can also use your own `Counter` implementation:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

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


{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

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

{% endhighlight %}
</div>

</div>

#### Gauge

A `Gauge` provides a value of any type on demand. In order to use a `Gauge` you must first create a class that implements the `org.apache.flink.metrics.Gauge` interface.
There is no restriction for the type of the returned value.
You can register a gauge by calling `gauge(String name, Gauge gauge)` on a `MetricGroup`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

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

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

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

{% endhighlight %}
</div>

</div>

Note that reporters will turn the exposed object into a `String`, which means that a meaningful `toString()` implementation is required.

#### Histogram

A `Histogram` measures the distribution of long values.
You can register one by calling `histogram(String name, Histogram histogram)` on a `MetricGroup`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

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

{% endhighlight %}
</div>

</div>

Flink does not provide a default implementation for `Histogram`, but offers a {% gh_link flink-metrics/flink-metrics-dropwizard/src/main/java/org/apache/flink/dropwizard/metrics/DropwizardHistogramWrapper.java "Wrapper" %} that allows usage of Codahale/DropWizard histograms.
To use this wrapper add the following dependency in your `pom.xml`:
{% highlight xml %}
<dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-metrics-dropwizard</artifactId>
      <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

You can then register a Codahale/DropWizard histogram like this:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

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

{% endhighlight %}
</div>

</div>

#### Meter

A `Meter` measures an average throughput. An occurrence of an event can be registered with the `markEvent()` method. Occurrence of multiple events at the same time can be registered with `markEvent(long n)` method.
You can register a meter by calling `meter(String name, Meter meter)` on a `MetricGroup`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

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

{% endhighlight %}
</div>

</div>

Flink offers a {% gh_link flink-metrics/flink-metrics-dropwizard/src/main/java/org/apache/flink/dropwizard/metrics/DropwizardMeterWrapper.java "Wrapper" %} that allows usage of Codahale/DropWizard meters.
To use this wrapper add the following dependency in your `pom.xml`:
{% highlight xml %}
<dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-metrics-dropwizard</artifactId>
      <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

You can then register a Codahale/DropWizard meter like this:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

class MyMapper extends RichMapFunction[Long,Long] {
  @transient private var meter: Meter = _

  override def open(config: Configuration): Unit = {
    com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter()
  
    meter = getRuntimeContext()
      .getMetricGroup()
      .meter("myMeter", new DropwizardMeterWrapper(dropwizardMeter))
  }

  override def map(value: Long): Long = {
    meter.markEvent()
    value
  }
}

{% endhighlight %}
</div>

</div>

## Scope

Every metric is assigned an identifier under which it will be reported that is based on 3 components: the user-provided name when registering the metric, an optional user-defined scope and a system-provided scope.
For example, if `A.B` is the system scope, `C.D` the user scope and `E` the name, then the identifier for the metric will be `A.B.C.D.E`.

You can configure which delimiter to use for the identifier (default: `.`) by setting the `metrics.scope.delimiter` key in `conf/flink-conf.yaml`.

### User Scope

You can define a user scope by calling either `MetricGroup#addGroup(String name)` or `MetricGroup#addGroup(int name)`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetrics")
  .counter("myCounter");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetrics")
  .counter("myCounter")

{% endhighlight %}
</div>

</div>

### System Scope

The system scope contains context information about the metric, for example in which task it was registered or what job that task belongs to.

Which context information should be included can be configured by setting the following keys in `conf/flink-conf.yaml`.
Each of these keys expect a format string that may contain constants (e.g. "taskmanager") and variables (e.g. "&lt;task_id&gt;") which will be replaced at runtime.

- `metrics.scope.jm`
  - Default: &lt;host&gt;.jobmanager
  - Applied to all metrics that were scoped to a job manager.
- `metrics.scope.jm.job`
  - Default: &lt;host&gt;.jobmanager.&lt;job_name&gt;
  - Applied to all metrics that were scoped to a job manager and job.
- `metrics.scope.tm`
  - Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;
  - Applied to all metrics that were scoped to a task manager.
- `metrics.scope.tm.job`
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

## Reporter

Metrics can be exposed to an external system by configuring one or several reporters in `conf/flink-conf.yaml`. These
reporters will be instantiated on each job and task manager when they are started.

- `metrics.reporters`: The list of named reporters.
- `metrics.reporter.<name>.<config>`: Generic setting `<config>` for the reporter named `<name>`.
- `metrics.reporter.<name>.class`: The reporter class to use for the reporter named `<name>`.
- `metrics.reporter.<name>.interval`: The reporter interval to use for the reporter named `<name>`.
- `metrics.reporter.<name>.scope.delimiter`: The delimiter to use for the identifier (default value use `metrics.scope.delimiter`) for the reporter named `<name>`.

All reporters must at least have the `class` property, some allow specifying a reporting `interval`. Below,
we will list more settings specific to each reporter.

Example reporter configuration that specifies multiple reporters:

```
metrics.reporters: my_jmx_reporter,my_other_reporter

metrics.reporter.my_jmx_reporter.class: org.apache.flink.metrics.jmx.JMXReporter
metrics.reporter.my_jmx_reporter.port: 9020-9040

metrics.reporter.my_other_reporter.class: org.apache.flink.metrics.graphite.GraphiteReporter
metrics.reporter.my_other_reporter.host: 192.168.1.1
metrics.reporter.my_other_reporter.port: 10000

```

**Important:** The jar containing the reporter must be accessible when Flink is started by placing it in the /lib folder.

You can write your own `Reporter` by implementing the `org.apache.flink.metrics.reporter.MetricReporter` interface.
If the Reporter should send out reports regularly you have to implement the `Scheduled` interface as well.

The following sections list the supported reporters.

### JMX (org.apache.flink.metrics.jmx.JMXReporter)

You don't have to include an additional dependency since the JMX reporter is available by default
but not activated.

Parameters:

- `port` - (optional) the port on which JMX listens for connections. This can also be a port range. When a
range is specified the actual port is shown in the relevant job or task manager log. If this setting is set
Flink will start an extra JMX connector for the given port/range. Metrics are always available on the default
local JMX interface.

Example configuration:

{% highlight yaml %}

metrics.reporters: jmx
metrics.reporter.jmx.class: org.apache.flink.metrics.jmx.JMXReporter
metrics.reporter.jmx.port: 8789

{% endhighlight %}

Metrics exposed through JMX are identified by a domain and a list of key-properties, which together form the object name.

The domain always begins with `org.apache.flink` followed by a generalized metric identifier. In contrast to the usual
identifier it is not affected by scope-formats, does not contain any variables and is constant across jobs.
An example for such a domain would be `org.apache.flink.job.task.numBytesOut`.

The key-property list contains the values for all variables, regardless of configured scope formats, that are associated
with a given metric.
An example for such a list would be `host=localhost,job_name=MyJob,task_name=MyTask`.

The domain thus identifies a metric class, while the key-property list identifies one (or multiple) instances of that metric.

### Ganglia (org.apache.flink.metrics.ganglia.GangliaReporter)

In order to use this reporter you must copy `/opt/flink-metrics-ganglia-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Parameters:

- `host` - the gmond host address configured under `udp_recv_channel.bind` in `gmond.conf`
- `port` - the gmond port configured under `udp_recv_channel.port` in `gmond.conf`
- `tmax` - soft limit for how long an old metric should be retained
- `dmax` - hard limit for how long an old metric should be retained
- `ttl` - time-to-live for transmitted UDP packets
- `addressingMode` - UDP addressing mode to use (UNICAST/MULTICAST)

Example configuration:

{% highlight yaml %}

metrics.reporters: gang
metrics.reporter.gang.class: org.apache.flink.metrics.ganglia.GangliaReporter
metrics.reporter.gang.host: localhost
metrics.reporter.gang.port: 8649
metrics.reporter.gang.tmax: 60
metrics.reporter.gang.dmax: 0
metrics.reporter.gang.ttl: 1
metrics.reporter.gang.addressingMode: MULTICAST

{% endhighlight %}

### Graphite (org.apache.flink.metrics.graphite.GraphiteReporter)

In order to use this reporter you must copy `/opt/flink-metrics-graphite-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Parameters:

- `host` - the Graphite server host
- `port` - the Graphite server port
- `protocol` - protocol to use (TCP/UDP)

Example configuration:

{% highlight yaml %}

metrics.reporters: grph
metrics.reporter.grph.class: org.apache.flink.metrics.graphite.GraphiteReporter
metrics.reporter.grph.host: localhost
metrics.reporter.grph.port: 2003
metrics.reporter.grph.protocol: TCP

{% endhighlight %}

### Prometheus (org.apache.flink.metrics.prometheus.PrometheusReporter)

In order to use this reporter you must copy `/opt/flink-metrics-prometheus-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Parameters:

- `port` - (optional) the port the Prometheus exporter listens on, defaults to [9249](https://github.com/prometheus/prometheus/wiki/Default-port-allocations). In order to be able to run several instances of the reporter on one host (e.g. when one TaskManager is colocated with the JobManager) it is advisable to use a port range like `9250-9260`.

Example configuration:

{% highlight yaml %}

metrics.reporters: prom
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter

{% endhighlight %}

Flink metric types are mapped to Prometheus metric types as follows: 

| Flink     | Prometheus | Note                                     |
| --------- |------------|------------------------------------------|
| Counter   | Gauge      |Prometheus counters cannot be decremented.|
| Gauge     | Gauge      |Only numbers and booleans are supported.  |
| Histogram | Summary    |Quantiles .5, .75, .95, .98, .99 and .999 |
| Meter     | Gauge      |The gauge exports the meter's rate.       |

All Flink metrics variables (see [List of all Variables](#list-of-all-variables)) are exported to Prometheus as labels. 

### StatsD (org.apache.flink.metrics.statsd.StatsDReporter)

In order to use this reporter you must copy `/opt/flink-metrics-statsd-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Parameters:

- `host` - the StatsD server host
- `port` - the StatsD server port

Example configuration:

{% highlight yaml %}

metrics.reporters: stsd
metrics.reporter.stsd.class: org.apache.flink.metrics.statsd.StatsDReporter
metrics.reporter.stsd.host: localhost
metrics.reporter.stsd.port: 8125

{% endhighlight %}

### Datadog (org.apache.flink.metrics.datadog.DatadogHttpReporter)

In order to use this reporter you must copy `/opt/flink-metrics-datadog-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Note any variables in Flink metrics, such as `<host>`, `<job_name>`, `<tm_id>`, `<subtask_index>`, `<task_name>`, and `<operator_name>`,
will be sent to Datadog as tags. Tags will look like `host:localhost` and `job_name:myjobname`.

Parameters:

- `apikey` - the Datadog API key
- `tags` - (optional) the global tags that will be applied to metrics when sending to Datadog. Tags should be separated by comma only

Example configuration:

{% highlight yaml %}

metrics.reporters: dghttp
metrics.reporter.dghttp.class: org.apache.flink.metrics.datadog.DatadogHttpReporter
metrics.reporter.dghttp.apikey: xxx
metrics.reporter.dghttp.tags: myflinkapp,prod

{% endhighlight %}


### Slf4j (org.apache.flink.metrics.slf4j.Slf4jReporter)

In order to use this reporter you must copy `/opt/flink-metrics-slf4j-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Example configuration:

{% highlight yaml %}

metrics.reporters: slf4j
metrics.reporter.slf4j.class: org.apache.flink.metrics.slf4j.Slf4jReporter
metrics.reporter.slf4j.interval: 60 SECONDS

{% endhighlight %}

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
      <th rowspan="12"><strong>Job-/TaskManager</strong></th>
      <td rowspan="12">Status.JVM.Memory</td>
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
      <td>The maximum amount of heap memory that can be used for memory management (in bytes).</td>
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
      <th rowspan="2"><strong>Job-/TaskManager</strong></th>
      <td rowspan="2">Status.JVM.GarbageCollector</td>
      <td>&lt;GarbageCollector&gt;.Count</td>
      <td>The total number of collections that have occurred.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>&lt;GarbageCollector&gt;.Time</td>
      <td>The total time spent performing garbage collection.</td>
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
      <th rowspan="8">Task</th>
      <td rowspan="4">buffers</td>
      <td>inputQueueLength</td>
      <td>The number of queued input buffers.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>outputQueueLength</td>
      <td>The number of queued output buffers.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inPoolUsage</td>
      <td>An estimate of the input buffers usage.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>outPoolUsage</td>
      <td>An estimate of the output buffers usage.</td>
      <td>Gauge</td>      
    </tr>
    <tr>
      <td rowspan="4">Network.&lt;Input|Output&gt;.&lt;gate&gt;<br />
        <strong>(only available if <tt>taskmanager.net.detailed-metrics</tt> config option is set)</strong></td>
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
      <th rowspan="4"><strong>JobManager</strong></th>
      <td>numRegisteredTaskManagers</td>
      <td>The number of registered taskmanagers.</td>
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
      <td>restartingTime</td>
      <td>The time it took to restart the job, or how long the current restart has been in progress (in milliseconds).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>uptime</td>
      <td>
        The time that the job has been running without interruption.
        <p>Returns -1 for completed jobs (in milliseconds).</p>
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>downtime</td>
      <td>
        For jobs currently in a failing/recovering situation, the time elapsed during this outage.
        <p>Returns 0 for running jobs and -1 for completed jobs (in milliseconds).</p>
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>fullRestarts</td>
      <td>The total number of full restarts since this job was submitted (in milliseconds).</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

### Checkpointing
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
      <th rowspan="9"><strong>Job (only available on JobManager)</strong></th>
      <td>lastCheckpointDuration</td>
      <td>The time it took to complete the last checkpoint (in milliseconds).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>lastCheckpointSize</td>
      <td>The total size of the last checkpoint (in bytes).</td>
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
      <td>lastCheckpointAlignmentBuffered</td>
      <td>The number of buffered bytes during alignment over all subtasks for the last checkpoint (in bytes).</td>
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
      <th rowspan="1">Task</th>
      <td>checkpointAlignmentTime</td>
      <td>The time in nanoseconds that the last barrier alignment took to complete, or how long the current alignment has taken so far (in nanoseconds).</td>
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
      <th rowspan="7"><strong>Task</strong></th>
      <td>currentLowWatermark</td>
      <td>The lowest watermark this task has received (in milliseconds).</td>
      <td>Gauge</td>
    </tr>
    <tr>
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
      <th rowspan="5"><strong>Task/Operator</strong></th>
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
      <th rowspan="2"><strong>Operator</strong></th>
      <td>latency</td>
      <td>The latency distributions from all incoming sources (in milliseconds).</td>
      <td>Histogram</td>
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
      <th rowspan="1">Operator</th>
      <td>commitsSucceeded</td>
      <td>Kafka offset commit success count if Kafka commit is turned on and checkpointing is enabled.</td>
      <td>Counter</td>
    </tr>
    <tr>
       <th rowspan="1">Operator</th>
       <td>commitsFailed</td>
       <td>Kafka offset commit failure count if Kafka commit is turned on and checkpointing is enabled.</td>
       <td>Counter</td>
    </tr>
  </tbody>
</table>

## Latency tracking

Flink allows to track the latency of records traveling through the system. To enable the latency tracking
a `latencyTrackingInterval` (in milliseconds) has to be set to a positive value in the `ExecutionConfig`.

At the `latencyTrackingInterval`, the sources will periodically emit a special record, called a `LatencyMarker`.
The marker contains a timestamp from the time when the record has been emitted at the sources.
Latency markers can not overtake regular user records, thus if records are queuing up in front of an operator, 
it will add to the latency tracked by the marker.

Note that the latency markers are not accounting for the time user records spend in operators as they are
bypassing them. In particular the markers are not accounting for the time records spend for example in window buffers.
Only if operators are not able to accept new records, thus they are queuing up, the latency measured using
the markers will reflect that.

All intermediate operators keep a list of the last `n` latencies from each source to compute 
a latency distribution.
The sink operators keep a list from each source, and each parallel source instance to allow detecting 
latency issues caused by individual machines.

Currently, Flink assumes that the clocks of all machines in the cluster are in sync. We recommend setting
up an automated clock synchronisation service (like NTP) to avoid false latency results.

## REST API integration

Metrics can be queried through the [Monitoring REST API]({{ site.baseurl }}/monitoring/rest_api.html).

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

Request a list of available metrics:

`GET /jobmanager/metrics`

~~~
[
  {
    "id": "metric1"
  },
  {
    "id": "metric2"
  }
]
~~~

Request the values for specific (unaggregated) metrics:

`GET taskmanagers/ABCDE/metrics?get=metric1,metric2`

~~~
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
~~~

Request aggregated values for specific metrics:

`GET /taskmanagers/metrics?get=metric1,metric2`

~~~
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
~~~

Request specific aggregated values for specific metrics:

`GET /taskmanagers/metrics?get=metric1,metric2&agg=min,max`

~~~
[
  {
    "id": "metric1",
    "min": 1,
    "max": 34,
  },
  {
    "id": "metric2",
    "min": 2,
    "max": 14,
  }
]
~~~

## Dashboard integration

Metrics that were gathered for each task or operator can also be visualized in the Dashboard. On the main page for a
job, select the `Metrics` tab. After selecting one of the tasks in the top graph you can select metrics to display using
the `Add Metric` drop-down menu.

* Task metrics are listed as `<subtask_index>.<metric_name>`.
* Operator metrics are listed as `<subtask_index>.<operator_name>.<metric_name>`.

Each metric will be visualized as a separate graph, with the x-axis representing time and the y-axis the measured value.
All graphs are automatically updated every 10 seconds, and continue to do so when navigating to another page.

There is no limit as to the number of visualized metrics; however only numeric metrics can be visualized.

{% top %}
