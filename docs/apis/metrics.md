---
title: "Metrics"
# Top-level navigation
top-nav-group: apis
top-nav-pos: 13
top-nav-title: "Metrics"
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

You can access the metric system from any user function that extends [RichFunction]({{ site.baseurl }}/apis/common/index.html#rich-functions) by calling `getRuntimeContext().getMetricGroup()`.
This method returns a `MetricGroup` object on which you can create and register new metrics.

### Metric types

Flink supports `Counters`, `Gauges` and `Histograms`.

#### Counter

A `Counter` is used to count something. The current value can be in- or decremented using `inc()/inc(long n)` or `dec()/dec(long n)`.
You can create and register a `Counter` by calling `counter(String name)` on a `MetricGroup`.

{% highlight java %}

public class MyMapper extends RichMapFunction<String, Integer> {
  private Counter counter;

  @Override
  public void open(Configuration config) {
    this.counter = getRuntimeContext()
      .getMetricGroup()
      .counter("myCounter");
  }

  @public Integer map(String value) throws Exception {
    this.counter.inc();
  }
}

{% endhighlight %}

Alternatively you can also use your own `Counter` implementation:

{% highlight java %}

public class MyMapper extends RichMapFunction<String, Integer> {
  private Counter counter;

  @Override
  public void open(Configuration config) {
    this.counter = getRuntimeContext()
      .getMetricGroup()
      .counter("myCustomCounter", new CustomCounter());
  }
}

{% endhighlight %}

#### Gauge

A `Gauge` provides a value of any type on demand. In order to use a `Gauge` you must first create a class that implements the `org.apache.flink.metrics.Gauge` interface.
There is no restriction for the type of the returned value.
You can register a gauge by calling `gauge(String name, Gauge gauge)` on a `MetricGroup`.

{% highlight java %}

public class MyMapper extends RichMapFunction<String, Integer> {
  private int valueToExpose;

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
}

{% endhighlight %}

Note that reporters will turn the exposed object into a `String`, which means that a meaningful `toString()` implementation is required.

#### Histogram

A `Histogram` measures the distribution of long values.
You can register one by calling `histogram(String name, Histogram histogram)` on a `MetricGroup`.

{% highlight java %}
public class MyMapper extends RichMapFunction<Long, Integer> {
  private Histogram histogram;

  @Override
  public void open(Configuration config) {
    this.histogram = getRuntimeContext()
      .getMetricGroup()
      .histogram("myHistogram", new MyHistogram());
  }

  @public Integer map(Long value) throws Exception {
    this.histogram.update(value);
  }
}
{% endhighlight %}

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

{% highlight java %}
public class MyMapper extends RichMapFunction<Long, Integer> {
  private Histogram histogram;

  @Override
  public void open(Configuration config) {
    com.codahale.metrics.Histogram histogram =
      new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

    this.histogram = getRuntimeContext()
      .getMetricGroup()
      .histogram("myHistogram", new DropWizardHistogramWrapper(histogram));
  }
}
{% endhighlight %}

## Scope

Every metric is assigned an identifier under which it will be reported that is based on 3 components: the user-provided name when registering the metric, an optional user-defined scope and a system-provided scope.
For example, if `A.B` is the sytem scope, `C.D` the user scope and `E` the name, then the identifier for the metric will be `A.B.C.D.E`.

You can configure which delimiter to use for the identifier (default: `.`) by setting the `metrics.scope.delimiter` key in `conf/flink-conf.yaml`.

### User Scope

You can define a user scope by calling either `MetricGroup#addGroup(String name)` or `MetricGroup#addGroup(int name)`.

{% highlight java %}

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetrics")
  .counter("myCounter");

{% endhighlight %}

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
- `metrics.scope.tm.task`
  - Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;task_name&gt;.&lt;subtask_index&gt;
   - Applied to all metrics that were scoped to a task.
- `metrics.scope.tm.operator`
  - Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;operator_name&gt;.&lt;subtask_index&gt;
  - Applied to all metrics that were scoped to an operator.

There are no restrictions on the number or order of variables. Variables are case sensitive.

The default scope for operator metrics will result in an identifier akin to `localhost.taskmanager.1234.MyJob.MyOperator.0.MyMetric`

If you also want to include the task name but omit the task manager information you can specify the following format:

`metrics.scope.tm.operator: <host>.<job_name>.<task_name>.<operator_name>.<subtask_index>`

This could create the identifier `localhost.MyJob.MySource_->_MyOperator.MyOperator.0.MyMetric`.

Note that for this format string an identifier clash can occur should the same job be run multiple times concurrently, which can lead to inconsistent metric data.
As such it is advised to either use format strings that provide a certain degree of uniqueness by including IDs (e.g &lt;job_id&gt;)
or by assigning unique names to jobs and operators.

### List of all Variables

- JobManager: &lt;host&gt;
- TaskManager: &lt;host&gt;, &lt;tm_id&gt;
- Job: &lt;job_id&gt;, &lt;job_name&gt;
- Task: &lt;task_id&gt;, &lt;task_name&gt;, &lt;task_attempt_id&gt;, &lt;task_attempt_num&gt;, &lt;subtask_index&gt;
- Operator: &lt;operator_name&gt;, &lt;subtask_index&gt;

## Reporter

Metrics can be exposed to an external system by configuring one or several reporters in `conf/flink-conf.yaml`.

- `metrics.reporters`: The list of named reporters.
- `metrics.reporter.<name>.<config>`: Generic setting `<config>` for the reporter named `<name>`.
- `metrics.reporter.<name>.class`: The reporter class to use for the reporter named `<name>`.
- `metrics.reporter.<name>.interval`: The reporter interval to use for the reporter named `<name>`.

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

You can write your own `Reporter` by implementing the `org.apache.flink.metrics.reporter.MetricReporter` interface.
If the Reporter should send out reports regularly you have to implement the `Scheduled` interface as well.

The following sections list the supported reporters.

### JMX (org.apache.flink.metrics.jmx.JMXReporter)

You don't have to include an additional dependency since the JMX reporter is available by default
but not activated.

Parameters:

- `port` - the port on which JMX listens for connections. This can also be a port range. When a
range is specified the actual port is shown in the relevant job or task manager log. If you don't
specify a port no extra JMX server will be started. Metrics are still available on the default
local JMX interface.

### Ganglia (org.apache.flink.metrics.ganglia.GangliaReporter)
Dependency:
{% highlight xml %}
<dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-metrics-ganglia</artifactId>
      <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

Parameters:

- `host` - the gmond host address configured under `udp_recv_channel.bind` in `gmond.conf`
- `port` - the gmond port configured under `udp_recv_channel.port` in `gmond.conf`
- `tmax` - soft limit for how long an old metric should be retained
- `dmax` - hard limit for how long an old metric should be retained
- `ttl` - time-to-live for transmitted UDP packets
- `addressingMode` - UDP addressing mode to use (UNICAST/MULTICAST)

### Graphite (org.apache.flink.metrics.graphite.GraphiteReporter)
Dependency:
{% highlight xml %}
<dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-metrics-graphite</artifactId>
      <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

Parameters:

- `host` - the Graphite server host
- `port` - the Graphite server port

### StatsD (org.apache.flink.metrics.statsd.StatsDReporter)
Dependency:
{% highlight xml %}
<dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-metrics-statsd</artifactId>
      <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

Parameters:

- `host` - the StatsD server host
- `port` - the StatsD server port

## System metrics

Flink exposes the following system metrics:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Scope</th>
      <th class="text-left">Metrics</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <th rowspan="1"><strong>JobManager</strong></th>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th rowspan="19"><strong>TaskManager.Status.JVM</strong></th>
      <td>ClassLoader.ClassesLoaded</td>
      <td>The total number of classes loaded since the start of the JVM.</td>
    </tr>
    <tr>
      <td>ClassLoader.ClassesUnloaded</td>
      <td>The total number of classes unloaded since the start of the JVM.</td>
    </tr>
    <tr>
      <td>GargabeCollector.&lt;garbageCollector&gt;.Count</td>
      <td>The total number of collections that have occurred.</td>
    </tr>
    <tr>
      <td>GargabeCollector.&lt;garbageCollector&gt;.Time</td>
      <td>The total time spent performing garbage collection.</td>
    </tr>
    <tr>
      <td>Memory.Heap.Used</td>
      <td>The amount of heap memory currently used.</td>
    </tr>
    <tr>
      <td>Memory.Heap.Committed</td>
      <td>The amount of heap memory guaranteed to be available to the JVM.</td>
    </tr>
    <tr>
      <td>Memory.Heap.Max</td>
      <td>The maximum amount of heap memory that can be used for memory management.</td>
    </tr>
    <tr>
      <td>Memory.NonHeap.Used</td>
      <td>The amount of non-heap memory currently used.</td>
    </tr>
    <tr>
      <td>Memory.NonHeap.Committed</td>
      <td>The amount of non-heap memory guaranteed to be available to the JVM.</td>
    </tr>
    <tr>
      <td>Memory.NonHeap.Max</td>
      <td>The maximum amount of non-heap memory that can be used for memory management.</td>
    </tr>
    <tr>
      <td>Memory.Direct.Count</td>
      <td>The number of buffers in the direct buffer pool.</td>
    </tr>
    <tr>
      <td>Memory.Direct.MemoryUsed</td>
      <td>The amount of memory used by the JVM for the direct buffer pool.</td>
    </tr>
    <tr>
      <td>Memory.Direct.TotalCapacity</td>
      <td>The total capacity of all buffers in the direct buffer pool.</td>
    </tr>
    <tr>
      <td>Memory.Mapped.Count</td>
      <td>The number of buffers in the mapped buffer pool.</td>
    </tr>
    <tr>
      <td>Memory.Mapped.MemoryUsed</td>
      <td>The amount of memory used by the JVM for the mapped buffer pool.</td>
    </tr>
    <tr>
      <td>Memory.Mapped.TotalCapacity</td>
      <td>The number of buffers in the mapped buffer pool.</td>
    </tr>
    <tr>
      <td>Threads.Count</td>
      <td>The total number of live threads.</td>
    </tr>
    <tr>
      <td>CPU.Load</td>
      <td>The recent CPu usage of the JVM.</td>
    </tr>
    <tr>
      <td>CPU.Time</td>
      <td>The CPU time used by the JVM.</td>
    </tr>
    <tr>
      <th rowspan="1"><strong>Job</strong></th>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <tr>
        <th rowspan="5"><strong>Task</strong></t>
        <td>currentLowWatermark</td>
        <td>The lowest watermark a task has received.</td>
      </tr>
      <tr>
        <td>lastCheckpointDuration</td>
        <td>The time it took to complete the last checkpoint.</td>
      </tr>
      <tr>
        <td>lastCheckpointSize</td>
        <td>The total size of the last checkpoint.</td>
      </tr>
      <tr>
        <td>restartingTime</td>
        <td>The time it took to restart the job.</td>
      </tr>
      <tr>
        <td>numBytesInLocal</td>
        <td>The total number of bytes this task has read from a local source.</td>
      </tr>
      <tr>
        <td>numBytesInRemote</td>
        <td>The total number of bytes this task has read from a remote source.</td>
      </tr>
      <tr>
        <td>numBytesOut</td>
        <td>The total number of bytes this task has emitted.</td>
      </tr>
    </tr>
    <tr>
      <tr>
        <th rowspan="3"><strong>Operator</strong></th>
        <td>numRecordsIn</td>
        <td>The total number of records this operator has received.</td>
      </tr>
      <tr>
        <td>numRecordsOut</td>
        <td>The total number of records this operator has emitted.</td>
      </tr>
      <tr>
        <td>numSplitsProcessed</td>
        <td>The total number of InputSplits this data source has processed.</td>
      </tr>
    </tr>
  </tbody>
</table>

{% top %}
