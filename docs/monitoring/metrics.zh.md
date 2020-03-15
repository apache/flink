---
title: "指标"
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

Every metric is assigned an identifier and a set of key-value pairs under which the metric will be reported.

The identifier is based on 3 components: a user-defined name when registering the metric, an optional user-defined scope and a system-provided scope.
For example, if `A.B` is the system scope, `C.D` the user scope and `E` the name, then the identifier for the metric will be `A.B.C.D.E`.

You can configure which delimiter to use for the identifier (default: `.`) by setting the `metrics.scope.delimiter` key in `conf/flink-conf.yaml`.

### User Scope

You can define a user scope by calling `MetricGroup#addGroup(String name)`, `MetricGroup#addGroup(int name)` or `MetricGroup#addGroup(String key, String value)`.
These methods affect what `MetricGroup#getMetricIdentifier` and `MetricGroup#getScopeComponents` return.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetrics")
  .counter("myCounter");

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetricsKey", "MyMetricsValue")
  .counter("myCounter");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetrics")
  .counter("myCounter")

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetricsKey", "MyMetricsValue")
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

### User Variables

You can define a user variable by calling `MetricGroup#addGroup(String key, String value)`.
This method affects what `MetricGroup#getMetricIdentifier`, `MetricGroup#getScopeComponents` and `MetricGroup#getAllVariables()` returns.

**Important:** User variables cannot be used in scope formats.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetricsKey", "MyMetricsValue")
  .counter("myCounter");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

counter = getRuntimeContext()
  .getMetricGroup()
  .addGroup("MyMetricsKey", "MyMetricsValue")
  .counter("myCounter")

{% endhighlight %}
</div>

</div>

## Reporter

Metrics can be exposed to an external system by configuring one or several reporters in `conf/flink-conf.yaml`. These
reporters will be instantiated on each job and task manager when they are started.

- `metrics.reporter.<name>.<config>`: Generic setting `<config>` for the reporter named `<name>`.
- `metrics.reporter.<name>.class`: The reporter class to use for the reporter named `<name>`.
- `metrics.reporter.<name>.factory.class`: The reporter factory class to use for the reporter named `<name>`.
- `metrics.reporter.<name>.interval`: The reporter interval to use for the reporter named `<name>`.
- `metrics.reporter.<name>.scope.delimiter`: The delimiter to use for the identifier (default value use `metrics.scope.delimiter`) for the reporter named `<name>`.
- `metrics.reporter.<name>.scope.variables.excludes`: (optional) A semicolon (;) separated list of variables that should be ignored by tag-based reporters. 
- `metrics.reporters`: (optional) A comma-separated include list of reporter names. By default all configured reporters will be used.

All reporters must at least have either the `class` or `factory.class` property. Which property may/should be used depends on the reporter implementation. See the individual reporter configuration sections for more information.
Some reporters (referred to as `Scheduled`) allow specifying a reporting `interval`.
Below more settings specific to each reporter will be listed.

Example reporter configuration that specifies multiple reporters:

{% highlight yaml %}
metrics.reporters: my_jmx_reporter,my_other_reporter

metrics.reporter.my_jmx_reporter.factory.class: org.apache.flink.metrics.jmx.JMXReporterFactory
metrics.reporter.my_jmx_reporter.port: 9020-9040

metrics.reporter.my_other_reporter.class: org.apache.flink.metrics.graphite.GraphiteReporter
metrics.reporter.my_other_reporter.host: 192.168.1.1
metrics.reporter.my_other_reporter.port: 10000

{% endhighlight %}

**Important:** The jar containing the reporter must be accessible when Flink is started by placing it in the /lib folder.

You can write your own `Reporter` by implementing the `org.apache.flink.metrics.reporter.MetricReporter` interface.
If the Reporter should send out reports regularly you have to implement the `Scheduled` interface as well.

The following sections list the supported reporters.

### JMX (org.apache.flink.metrics.jmx.JMXReporter)

You don't have to include an additional dependency since the JMX reporter is available by default
but not activated.

Parameters:

- `port` - (optional) the port on which JMX listens for connections.
In order to be able to run several instances of the reporter on one host (e.g. when one TaskManager is colocated with the JobManager) it is advisable to use a port range like `9250-9260`.
When a range is specified the actual port is shown in the relevant job or task manager log.
If this setting is set Flink will start an extra JMX connector for the given port/range.
Metrics are always available on the default local JMX interface.

Example configuration:

{% highlight yaml %}

metrics.reporter.jmx.factory.class: org.apache.flink.metrics.jmx.JMXReporterFactory
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

### Graphite (org.apache.flink.metrics.graphite.GraphiteReporter)

In order to use this reporter you must copy `/opt/flink-metrics-graphite-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Parameters:

- `host` - the Graphite server host
- `port` - the Graphite server port
- `protocol` - protocol to use (TCP/UDP)

Example configuration:

{% highlight yaml %}

metrics.reporter.grph.class: org.apache.flink.metrics.graphite.GraphiteReporter
metrics.reporter.grph.host: localhost
metrics.reporter.grph.port: 2003
metrics.reporter.grph.protocol: TCP

{% endhighlight %}

### InfluxDB (org.apache.flink.metrics.influxdb.InfluxdbReporter)

In order to use this reporter you must copy `/opt/flink-metrics-influxdb-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Parameters:

- `host` - the InfluxDB server host
- `port` - (optional) the InfluxDB server port, defaults to `8086`
- `db` - the InfluxDB database to store metrics
- `username` - (optional) InfluxDB username used for authentication
- `password` - (optional) InfluxDB username's password used for authentication
- `retentionPolicy` - (optional) InfluxDB retention policy, defaults to retention policy defined on the server for the db
- `consistency` - (optional) InfluxDB consistency level for metrics. Possible values: [ALL, ANY, ONE, QUORUM], default is ONE
- `connectTimeout` - (optional) the InfluxDB client connect timeout in milliseconds, default is 10000 ms
- `writeTimeout` - (optional) the InfluxDB client write timeout in milliseconds, default is 10000 ms

Example configuration:

{% highlight yaml %}

metrics.reporter.influxdb.class: org.apache.flink.metrics.influxdb.InfluxdbReporter
metrics.reporter.influxdb.host: localhost
metrics.reporter.influxdb.port: 8086
metrics.reporter.influxdb.db: flink
metrics.reporter.influxdb.username: flink-metrics
metrics.reporter.influxdb.password: qwerty
metrics.reporter.influxdb.retentionPolicy: one_hour
metrics.reporter.influxdb.consistency: ANY
metrics.reporter.influxdb.connectTimeout: 60000
metrics.reporter.influxdb.writeTimeout: 60000

{% endhighlight %}

The reporter would send metrics using http protocol to the InfluxDB server with the specified retention policy (or the default policy specified on the server).
All Flink metrics variables (see [List of all Variables](#list-of-all-variables)) are exported as InfluxDB tags.

### Prometheus (org.apache.flink.metrics.prometheus.PrometheusReporter)

In order to use this reporter you must copy `/opt/flink-metrics-prometheus{{site.scala_version_suffix}}-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Parameters:

- `port` - (optional) the port the Prometheus exporter listens on, defaults to [9249](https://github.com/prometheus/prometheus/wiki/Default-port-allocations). In order to be able to run several instances of the reporter on one host (e.g. when one TaskManager is colocated with the JobManager) it is advisable to use a port range like `9250-9260`.
- `filterLabelValueCharacters` - (optional) Specifies whether to filter label value characters. If enabled, all characters not matching \[a-zA-Z0-9:_\] will be removed, otherwise no characters will be removed. Before disabling this option please ensure that your label values meet the [Prometheus requirements](https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels).

Example configuration:

{% highlight yaml %}

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

### PrometheusPushGateway (org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter)

In order to use this reporter you must copy `/opt/flink-metrics-prometheus-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Parameters:

{% include generated/prometheus_push_gateway_reporter_configuration.html %}

Example configuration:

{% highlight yaml %}

metrics.reporter.promgateway.class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter
metrics.reporter.promgateway.host: localhost
metrics.reporter.promgateway.port: 9091
metrics.reporter.promgateway.jobName: myJob
metrics.reporter.promgateway.randomJobNameSuffix: true
metrics.reporter.promgateway.deleteOnShutdown: false
metrics.reporter.promgateway.groupingKey: k1=v1;k2=v2

{% endhighlight %}

The PrometheusPushGatewayReporter pushes metrics to a [Pushgateway](https://github.com/prometheus/pushgateway), which can be scraped by Prometheus.

Please see the [Prometheus documentation](https://prometheus.io/docs/practices/pushing/) for use-cases.

### StatsD (org.apache.flink.metrics.statsd.StatsDReporter)

In order to use this reporter you must copy `/opt/flink-metrics-statsd-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Parameters:

- `host` - the StatsD server host
- `port` - the StatsD server port

Example configuration:

{% highlight yaml %}

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
- `proxyHost` - (optional) The proxy host to use when sending to Datadog.
- `proxyPort` - (optional) The proxy port to use when sending to Datadog, defaults to 8080.

Example configuration:

{% highlight yaml %}

metrics.reporter.dghttp.class: org.apache.flink.metrics.datadog.DatadogHttpReporter
metrics.reporter.dghttp.apikey: xxx
metrics.reporter.dghttp.tags: myflinkapp,prod
metrics.reporter.dghttp.proxyHost: my.web.proxy.com
metrics.reporter.dghttp.proxyPort: 8080

{% endhighlight %}


### Slf4j (org.apache.flink.metrics.slf4j.Slf4jReporter)

In order to use this reporter you must copy `/opt/flink-metrics-slf4j-{{site.version}}.jar` into the `/lib` folder
of your Flink distribution.

Example configuration:

{% highlight yaml %}

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


### Network (Deprecated: use [Default shuffle service metrics]({{ site.baseurl }}/monitoring/metrics.html#default-shuffle-service))
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
      <td>An estimate of the output buffers usage.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="4">Network.&lt;Input|Output&gt;.&lt;gate|partition&gt;<br />
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
      <th rowspan="2"><strong>TaskManager</strong></th>
      <td rowspan="2">Status.Shuffle.Netty</td>
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
      <td rowspan="2">Shuffle.Netty.Input.Buffers</td>
      <td>inputQueueLength</td>
      <td>The number of queued input buffers.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>inPoolUsage</td>
      <td>An estimate of the input buffers usage.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="2">Shuffle.Netty.Output.Buffers</td>
      <td>outputQueueLength</td>
      <td>The number of queued output buffers.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>outPoolUsage</td>
      <td>An estimate of the output buffers usage.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="4">Shuffle.Netty.&lt;Input|Output&gt;.&lt;gate|partition&gt;<br />
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
    <tr>
      <th rowspan="8"><strong>Task</strong></th>
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
      <th rowspan="5"><strong>Job (only available on JobManager)</strong></th>
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

### RocksDB
Certain RocksDB native metrics are available but disabled by default, you can find full documentation [here]({{ site.baseurl }}/ops/config.html#rocksdb-native-metrics)

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
      <td>The latency distributions from a given source (subtask) to an operator subtask (in milliseconds), depending on the <a href="{{ site.baseurl }}/zh/ops/config.html#metrics-latency-granularity">latency granularity</a>.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <th rowspan="13"><strong>Task</strong></th>
      <td>numBytesInLocal</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{ site.baseurl }}/monitoring/metrics.html#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesInLocalPerSecond</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{ site.baseurl }}/monitoring/metrics.html#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBytesInRemote</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{ site.baseurl }}/monitoring/metrics.html#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBytesInRemotePerSecond</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{ site.baseurl }}/monitoring/metrics.html#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersInLocal</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{ site.baseurl }}/monitoring/metrics.html#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersInLocalPerSecond</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{ site.baseurl }}/monitoring/metrics.html#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>numBuffersInRemote</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{ site.baseurl }}/monitoring/metrics.html#default-shuffle-service">Default shuffle service metrics</a>.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>numBuffersInRemotePerSecond</td>
      <td><span class="label label-danger">Attention:</span> deprecated, use <a href="{{ site.baseurl }}/monitoring/metrics.html#default-shuffle-service">Default shuffle service metrics</a>.</td>
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
      <td>currentInput1Watermark</td>
      <td>
        The last watermark this operator has received in its first input (in milliseconds).
        <p><strong>Note:</strong> Only for operators with 2 inputs.</p>
      </td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>currentInput2Watermark</td>
      <td>
        The last watermark this operator has received in its second input (in milliseconds).
        <p><strong>Note:</strong> Only for operators with 2 inputs.</p>
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
      <td>commitsSucceeded</td>
      <td>n/a</td>
      <td>The total number of successful offset commits to Kafka, if offset committing is turned on and checkpointing is enabled.</td>
      <td>Counter</td>
    </tr>
    <tr>
       <th rowspan="1">Operator</th>
       <td>commitsFailed</td>
       <td>n/a</td>
       <td>The total number of offset commit failures to Kafka, if offset committing is
       turned on and checkpointing is enabled. Note that committing offsets back to Kafka
       is only a means to expose consumer progress, so a commit failure does not affect
       the integrity of Flink's checkpointed partition offsets.</td>
       <td>Counter</td>
    </tr>
    <tr>
       <th rowspan="1">Operator</th>
       <td>committedOffsets</td>
       <td>topic, partition</td>
       <td>The last successfully committed offsets to Kafka, for each partition.
       A particular partition's metric can be specified by topic name and partition id.</td>
       <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="1">Operator</th>
      <td>currentOffsets</td>
      <td>topic, partition</td>
      <td>The consumer's current read offset, for each partition. A particular
      partition's metric can be specified by topic name and partition id.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

#### Kinesis Connectors
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

### System resources

System resources reporting is disabled by default. When `metrics.system-resource`
is enabled additional metrics listed below will be available on Job- and TaskManager.
System resources metrics are updated periodically and they present average values for a
configured interval (`metrics.system-resource-probing-interval`).

System resources reporting requires an optional dependency to be present on the
classpath (for example placed in Flink's `lib` directory):

  - `com.github.oshi:oshi-core:3.4.0` (licensed under EPL 1.0 license)

Including it's transitive dependencies:

  - `net.java.dev.jna:jna-platform:jar:4.2.2`
  - `net.java.dev.jna:jna:jar:4.2.2`

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
      <th rowspan="12"><strong>Job-/TaskManager</strong></th>
      <td rowspan="12">System.CPU</td>
      <td>Usage</td>
      <td>Overall % of CPU usage on the machine.</td>
    </tr>
    <tr>
      <td>Idle</td>
      <td>% of CPU Idle usage on the machine.</td>
    </tr>
    <tr>
      <td>Sys</td>
      <td>% of System CPU usage on the machine.</td>
    </tr>
    <tr>
      <td>User</td>
      <td>% of User CPU usage on the machine.</td>
    </tr>
    <tr>
      <td>IOWait</td>
      <td>% of IOWait CPU usage on the machine.</td>
    </tr>
    <tr>
      <td>Irq</td>
      <td>% of Irq CPU usage on the machine.</td>
    </tr>
    <tr>
      <td>SoftIrq</td>
      <td>% of SoftIrq CPU usage on the machine.</td>
    </tr>
    <tr>
      <td>Nice</td>
      <td>% of Nice Idle usage on the machine.</td>
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

## Latency tracking

Flink allows to track the latency of records travelling through the system. This feature is disabled by default.
To enable the latency tracking you must set the `latencyTrackingInterval` to a positive number in either the
[Flink configuration]({{ site.baseurl }}/ops/config.html#metrics-latency-interval) or `ExecutionConfig`.

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
be controlled in the [Flink configuration]({{ site.baseurl }}/ops/config.html#metrics-latency-interval). For the highest
granularity `subtask` Flink will derive the latency distribution between every source subtask and every downstream 
subtask, which results in quadratic (in the terms of the parallelism) number of histograms. 

Currently, Flink assumes that the clocks of all machines in the cluster are in sync. We recommend setting
up an automated clock synchronisation service (like NTP) to avoid false latency results.

<span class="label label-danger">Warning</span> Enabling latency metrics can significantly impact the performance
of the cluster (in particular for `subtask` granularity). It is highly recommended to only use them for debugging 
purposes.

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

{% highlight json %}
[
  {
    "id": "metric1"
  },
  {
    "id": "metric2"
  }
]
{% endhighlight %}

Request the values for specific (unaggregated) metrics:

`GET taskmanagers/ABCDE/metrics?get=metric1,metric2`

{% highlight json %}
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
{% endhighlight %}

Request aggregated values for specific metrics:

`GET /taskmanagers/metrics?get=metric1,metric2`

{% highlight json %}
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
{% endhighlight %}

Request specific aggregated values for specific metrics:

`GET /taskmanagers/metrics?get=metric1,metric2&agg=min,max`

{% highlight json %}
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
{% endhighlight %}

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
