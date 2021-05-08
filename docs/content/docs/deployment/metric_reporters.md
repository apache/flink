---
title: "Metric Reporters"
weight: 7
type: docs
aliases:
  - /deployment/metric_reporters.html
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

# Metric Reporters

Flink allows reporting metrics to external systems.
For more information about Flink's metric system go to the [metric system documentation]({{< ref "docs/ops/metrics" >}}).


## Reporter

Metrics can be exposed to an external system by configuring one or several reporters in `conf/flink-conf.yaml`. These
reporters will be instantiated on each job and task manager when they are started.

- `metrics.reporter.<name>.<config>`: Generic setting `<config>` for the reporter named `<name>`.
- `metrics.reporter.<name>.class`: The reporter class to use for the reporter named `<name>`.
- `metrics.reporter.<name>.factory.class`: The reporter factory class to use for the reporter named `<name>`.
- `metrics.reporter.<name>.interval`: The reporter interval to use for the reporter named `<name>`.
- `metrics.reporter.<name>.scope.delimiter`: The delimiter to use for the identifier (default value use `metrics.scope.delimiter`) for the reporter named `<name>`.
- `metrics.reporter.<name>.scope.variables.excludes`: (optional) A semi-colon (;) separate list of variables that should be ignored by tag-based reporters (e.g., Prometheus, InfluxDB). 
- `metrics.reporters`: (optional) A comma-separated include list of reporter names. By default all configured reporters will be used.

All reporters must at least have either the `class` or `factory.class` property. Which property may/should be used depends on the reporter implementation. See the individual reporter configuration sections for more information.
Some reporters (referred to as `Scheduled`) allow specifying a reporting `interval`.
Below more settings specific to each reporter will be listed.

Example reporter configuration that specifies multiple reporters:

```yaml
metrics.reporters: my_jmx_reporter,my_other_reporter

metrics.reporter.my_jmx_reporter.factory.class: org.apache.flink.metrics.jmx.JMXReporterFactory
metrics.reporter.my_jmx_reporter.port: 9020-9040
metrics.reporter.my_jmx_reporter.scope.variables.excludes:job_id;task_attempt_num

metrics.reporter.my_other_reporter.class: org.apache.flink.metrics.graphite.GraphiteReporter
metrics.reporter.my_other_reporter.host: 192.168.1.1
metrics.reporter.my_other_reporter.port: 10000

```

**Important:** The jar containing the reporter must be accessible when Flink is started. Reporters that support the
 `factory.class` property can be loaded as [plugins]({{< ref "docs/deployment/filesystems/plugins" >}}). Otherwise the jar must be placed
 in the /lib folder. Reporters that are shipped with Flink (i.e., all reporters documented on this page) are available
 by default.

You can write your own `Reporter` by implementing the `org.apache.flink.metrics.reporter.MetricReporter` interface.
If the Reporter should send out reports regularly you have to implement the `Scheduled` interface as well.
By additionally implementing a `MetricReporterFactory` your reporter can also be loaded as a plugin.

The following sections list the supported reporters.

### JMX 
#### (org.apache.flink.metrics.jmx.JMXReporter)

You don't have to include an additional dependency since the JMX reporter is available by default
but not activated.

Parameters:

- `port` - (optional) the port on which JMX listens for connections.
In order to be able to run several instances of the reporter on one host (e.g. when one TaskManager is colocated with the JobManager) it is advisable to use a port range like `9250-9260`.
When a range is specified the actual port is shown in the relevant job or task manager log.
If this setting is set Flink will start an extra JMX connector for the given port/range.
Metrics are always available on the default local JMX interface.

Example configuration:

```yaml

metrics.reporter.jmx.factory.class: org.apache.flink.metrics.jmx.JMXReporterFactory
metrics.reporter.jmx.port: 8789

```

Metrics exposed through JMX are identified by a domain and a list of key-properties, which together form the object name.

The domain always begins with `org.apache.flink` followed by a generalized metric identifier. In contrast to the usual
identifier it is not affected by scope-formats, does not contain any variables and is constant across jobs.
An example for such a domain would be `org.apache.flink.job.task.numBytesOut`.

The key-property list contains the values for all variables, regardless of configured scope formats, that are associated
with a given metric.
An example for such a list would be `host=localhost,job_name=MyJob,task_name=MyTask`.

The domain thus identifies a metric class, while the key-property list identifies one (or multiple) instances of that metric.

### Graphite
#### (org.apache.flink.metrics.graphite.GraphiteReporter)

Parameters:

- `host` - the Graphite server host
- `port` - the Graphite server port
- `protocol` - protocol to use (TCP/UDP)

Example configuration:

```yaml

metrics.reporter.grph.factory.class: org.apache.flink.metrics.graphite.GraphiteReporterFactory
metrics.reporter.grph.host: localhost
metrics.reporter.grph.port: 2003
metrics.reporter.grph.protocol: TCP
metrics.reporter.grph.interval: 60 SECONDS

```

### InfluxDB
#### (org.apache.flink.metrics.influxdb.InfluxdbReporter)

In order to use this reporter you must copy `/opt/flink-metrics-influxdb-{{< version >}}.jar` into the `plugins/influxdb` folder
of your Flink distribution.

Parameters:

{{< generated/influxdb_reporter_configuration >}}

Example configuration:

```yaml

metrics.reporter.influxdb.factory.class: org.apache.flink.metrics.influxdb.InfluxdbReporterFactory
metrics.reporter.influxdb.scheme: http
metrics.reporter.influxdb.host: localhost
metrics.reporter.influxdb.port: 8086
metrics.reporter.influxdb.db: flink
metrics.reporter.influxdb.username: flink-metrics
metrics.reporter.influxdb.password: qwerty
metrics.reporter.influxdb.retentionPolicy: one_hour
metrics.reporter.influxdb.consistency: ANY
metrics.reporter.influxdb.connectTimeout: 60000
metrics.reporter.influxdb.writeTimeout: 60000
metrics.reporter.influxdb.interval: 60 SECONDS

```

The reporter would send metrics using http protocol to the InfluxDB server with the specified retention policy (or the default policy specified on the server).
All Flink metrics variables (see [List of all Variables]({{< ref "docs/ops/metrics" >}}#list-of-all-variables)) are exported as InfluxDB tags.

### Prometheus
#### (org.apache.flink.metrics.prometheus.PrometheusReporter)

Parameters:

- `port` - (optional) the port the Prometheus exporter listens on, defaults to [9249](https://github.com/prometheus/prometheus/wiki/Default-port-allocations). In order to be able to run several instances of the reporter on one host (e.g. when one TaskManager is colocated with the JobManager) it is advisable to use a port range like `9250-9260`.
- `filterLabelValueCharacters` - (optional) Specifies whether to filter label value characters. If enabled, all characters not matching \[a-zA-Z0-9:_\] will be removed, otherwise no characters will be removed. Before disabling this option please ensure that your label values meet the [Prometheus requirements](https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels).

Example configuration:

```yaml

metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter

```

Flink metric types are mapped to Prometheus metric types as follows: 

| Flink     | Prometheus | Note                                     |
| --------- |------------|------------------------------------------|
| Counter   | Gauge      |Prometheus counters cannot be decremented.|
| Gauge     | Gauge      |Only numbers and booleans are supported.  |
| Histogram | Summary    |Quantiles .5, .75, .95, .98, .99 and .999 |
| Meter     | Gauge      |The gauge exports the meter's rate.       |

All Flink metrics variables (see [List of all Variables]({{< ref "docs/ops/metrics" >}}#list-of-all-variables)) are exported to Prometheus as labels. 

### PrometheusPushGateway 
#### (org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter)

Parameters:

{{< generated/prometheus_push_gateway_reporter_configuration >}}

Example configuration:

```yaml

metrics.reporter.promgateway.class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter
metrics.reporter.promgateway.host: localhost
metrics.reporter.promgateway.port: 9091
metrics.reporter.promgateway.jobName: myJob
metrics.reporter.promgateway.randomJobNameSuffix: true
metrics.reporter.promgateway.deleteOnShutdown: false
metrics.reporter.promgateway.groupingKey: k1=v1;k2=v2
metrics.reporter.promgateway.interval: 60 SECONDS

```

The PrometheusPushGatewayReporter pushes metrics to a [Pushgateway](https://github.com/prometheus/pushgateway), which can be scraped by Prometheus.

Please see the [Prometheus documentation](https://prometheus.io/docs/practices/pushing/) for use-cases.

### StatsD
#### (org.apache.flink.metrics.statsd.StatsDReporter)

Parameters:

- `host` - the StatsD server host
- `port` - the StatsD server port

Example configuration:

```yaml

metrics.reporter.stsd.factory.class: org.apache.flink.metrics.statsd.StatsDReporterFactory
metrics.reporter.stsd.host: localhost
metrics.reporter.stsd.port: 8125
metrics.reporter.stsd.interval: 60 SECONDS

```

### Datadog
#### (org.apache.flink.metrics.datadog.DatadogHttpReporter)

Note any variables in Flink metrics, such as `<host>`, `<job_name>`, `<tm_id>`, `<subtask_index>`, `<task_name>`, and `<operator_name>`,
will be sent to Datadog as tags. Tags will look like `host:localhost` and `job_name:myjobname`.

<span class="label label-info">Note</span> Histograms are exposed as a series of gauges following the naming convention of Datadog histograms (`<metric_name>.<aggregation>`).
The `min` aggregation is reported by default, whereas `sum` is not available.
In contrast to Datadog-provided Histograms the reported aggregations are not computed for a specific reporting interval.

Parameters:

- `apikey` - the Datadog API key
- `tags` - (optional) the global tags that will be applied to metrics when sending to Datadog. Tags should be separated by comma only
- `proxyHost` - (optional) The proxy host to use when sending to Datadog.
- `proxyPort` - (optional) The proxy port to use when sending to Datadog, defaults to 8080.
- `dataCenter` - (optional) The data center (`EU`/`US`) to connect to, defaults to `US`.
- `maxMetricsPerRequest` - (optional) The maximum number of metrics to include in each request, defaults to 2000.

Example configuration:

```yaml

metrics.reporter.dghttp.factory.class: org.apache.flink.metrics.datadog.DatadogHttpReporterFactory
metrics.reporter.dghttp.apikey: xxx
metrics.reporter.dghttp.tags: myflinkapp,prod
metrics.reporter.dghttp.proxyHost: my.web.proxy.com
metrics.reporter.dghttp.proxyPort: 8080
metrics.reporter.dghttp.dataCenter: US
metrics.reporter.dghttp.maxMetricsPerRequest: 2000
metrics.reporter.dghttp.interval: 60 SECONDS

```


### Slf4j
#### (org.apache.flink.metrics.slf4j.Slf4jReporter)

Example configuration:

```yaml

metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory
metrics.reporter.slf4j.interval: 60 SECONDS

```
{{< top >}}
