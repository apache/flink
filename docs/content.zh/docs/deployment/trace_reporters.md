---
title: "Trace Reporters"
weight: 7
type: docs
aliases:
  - /deployment/trace_reporters.html
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

# Trace Reporters

Flink allows reporting traces to external systems.
For more information about Flink's tracing system go to the [tracing system documentation]({{< ref "docs/ops/traces" >}}).

Traces can be exposed to an external system by configuring one or several reporters in [Flink configuration file]({{< ref "docs/deployment/config#flink-配置文件" >}}). These
reporters will be instantiated on each job and task manager when they are started.

Below is a list of parameters that are generally applicable to all reporters.
All properties are configured by setting `traces.reporter.<reporter_name>.<property>` in the configuration.
Reporters may additionally offer implementation-specific parameters, which are documented in the respective reporter's section. 

{{< include_reporter_config "layouts/shortcodes/generated/trace_reporters_section.html" >}}

All reporter configurations must contain the `factory.class` property.

Example reporter configuration that specifies multiple reporters:

```yaml
traces.reporters: otel,my_other_otel

traces.reporter.otel.factory.class: org.apache.flink.common.metrics.OpenTelemetryTraceReporterFactory
traces.reporter.otel.exporter.endpoint: http://127.0.0.1:1337
traces.reporter.otel.scope.variables.additional: region:eu-west-1,environment:local,flink_runtime:1.17.1

traces.reporter.my_other_otel.factory.class: org.apache.flink.common.metrics.OpenTelemetryTraceReporterFactory
traces.reporter.my_other_otel.exporter.endpoint: http://196.168.0.1:31337
```

**Important:** The jar containing the reporter must be accessible when Flink is started.
 Reporters are loaded as [plugins]({{< ref "docs/deployment/filesystems/plugins" >}}).
 All reporters documented on this page are available by default.

You can write your own `Reporter` by implementing the `org.apache.flink.traces.reporter.TraceReporter` and `org.apache.flink.traces.reporter.TraceReporterFactory` interfaces.
Be careful that all the method must not block for a significant amount of time, and any reporter needing more time should instead run the operation asynchronously.

## Reporters

The following sections list the supported reporters.

### Slf4j
#### (org.apache.flink.traces.slf4j.Slf4jTraceReporter)

Example configuration:

```yaml
traces.reporter.slf4j.factory.class: org.apache.flink.traces.slf4j.Slf4jTraceReporterFactory
```
{{< top >}}
