---
title: "Metrics"
weight: 111
type: docs
aliases:
  - /dev/python/table-api-users-guide/metrics.html
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

PyFlink exposes a metric system that allows gathering and exposing metrics to external systems.

## Registering metrics

You can access the metric system from a [Python user-defined function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}})
by calling `function_context.get_metric_group()` in the `open` method.
The `get_metric_group()` method returns a `MetricGroup` object on which you can create
and register new metrics.

### Metric types

PyFlink supports `Counters`, `Gauges`, `Distribution` and `Meters`.

#### Counter

A `Counter` is used to count something. The current value can be in- or decremented using `inc()/inc(n: int)` or `dec()/dec(n: int)`.
You can create and register a `Counter` by calling `counter(name: str)` on a `MetricGroup`.

{{< tabs "eb8f7741-9cdd-4027-b5b5-7a5b9c776741" >}}
{{< tab "Python" >}}
```python
from pyflink.table.udf import ScalarFunction

class MyUDF(ScalarFunction):

    def __init__(self):
        self.counter = None

    def open(self, function_context):
        self.counter = function_context.get_metric_group().counter("my_counter")

    def eval(self, i):
        self.counter.inc(i)
        return i

```
{{< /tab >}}
{{< /tabs >}}

#### Gauge

A `Gauge` provides a value on demand. You can register a gauge by calling
`gauge(name: str, obj: Callable[[], int])` on a MetricGroup. The Callable object will be used to
report the values. Gauge metrics are restricted to integer-only values.

{{< tabs "c2c7851f-a881-4521-94d5-70ab5d87f573" >}}
{{< tab "Python" >}}
```python
from pyflink.table.udf import ScalarFunction

class MyUDF(ScalarFunction):

    def __init__(self):
        self.length = 0

    def open(self, function_context):
        function_context.get_metric_group().gauge("my_gauge", lambda : self.length)

    def eval(self, i):
        self.length = i
        return i - 1
```
{{< /tab >}}
{{< /tabs >}}

#### Distribution

A metric that reports information(sum, count, min, max and mean) about the distribution of
reported values. The value can be updated using `update(n: int)`. You can register a distribution
by calling `distribution(name: str)` on a MetricGroup. Distribution metrics are restricted to
integer-only distributions.

{{< tabs "f42f5b3b-c39c-4cc7-9c96-fed6858c9466" >}}
{{< tab "Python" >}}
```python
from pyflink.table.udf import ScalarFunction

class MyUDF(ScalarFunction):

    def __init__(self):
        self.distribution = None

    def open(self, function_context):
        self.distribution = function_context.get_metric_group().distribution("my_distribution")

    def eval(self, i):
        self.distribution.update(i)
        return i - 1
```
{{< /tab >}}
{{< /tabs >}}

#### Meter

A Meter measures an average throughput. An occurrence of an event can be registered with the
`mark_event()` method. The occurrence of multiple events at the same time can be registered with
mark_event(n: int) method. You can register a meter by calling
`meter(self, name: str, time_span_in_seconds: int = 60)` on a MetricGroup.
The default value of time_span_in_seconds is 60.

{{< tabs "63b2e154-fd88-4e13-9a52-96f4ca117ff2" >}}
{{< tab "Python" >}}
```python
from pyflink.table.udf import ScalarFunction

class MyUDF(ScalarFunction):

    def __init__(self):
        self.meter = None

    def open(self, function_context):
        super().open(function_context)
        # an average rate of events per second over 120s, default is 60s.
        self.meter = function_context.get_metric_group().meter("my_meter", time_span_in_seconds=120)

    def eval(self, i):
        self.meter.mark_event(i)
        return i - 1
```
{{< /tab >}}
{{< /tabs >}}

## Scope

You can refer to the Java metric document for more details on [Scope definition]({{< ref "docs/ops/metrics" >}}#Scope).

### User Scope

You can define a user scope by calling `MetricGroup.add_group(key: str, value: str = None)`.
If `value` is not `None`, creates a new key-value MetricGroup pair.
The key group is added to this group's sub-groups, while the value group is added to the key
group's sub-groups. In this case, the value group will be returned, and a user variable will be defined.

{{< tabs "a3040b2d-bf2d-4ce4-be2f-2896f48334c8" >}}
{{< tab "Python" >}}
```python

function_context
    .get_metric_group()
    .add_group("my_metrics")
    .counter("my_counter")

function_context
    .get_metric_group()
    .add_group("my_metrics_key", "my_metrics_value")
    .counter("my_counter")

```
{{< /tab >}}
{{< /tabs >}}

### System Scope

You can refer to the Java metric document for more details on [System Scope]({{< ref "docs/ops/metrics" >}}#system-scope).

### List of all Variables

You can refer to the Java metric document for more details on [List of all Variables]({{< ref "docs/ops/metrics" >}}#list-of-all-variables).

### User Variables

You can define a user variable by calling `MetricGroup.addGroup(key: str, value: str = None)` and
specifying the value parameter.

**Important:** User variables cannot be used in scope formats.

{{< tabs "d27cbda0-da5f-4a77-a02e-2e54e3156e31" >}}
{{< tab "Python" >}}
```python
function_context
    .get_metric_group()
    .add_group("my_metrics_key", "my_metrics_value")
    .counter("my_counter")
```
{{< /tab >}}
{{< /tabs >}}

## Common part between PyFlink and Flink

You can refer to the Java metric document for more details on the following sections:

- [Reporter]({{< ref "docs/deployment/metric_reporters" >}}).
- [System metrics]({{< ref "docs/ops/metrics" >}}#system-metrics).
- [Latency tracking]({{< ref "docs/ops/metrics" >}}#latency-tracking).
- [REST API integration]({{< ref "docs/ops/metrics" >}}#rest-api-integration).
- [Dashboard integration]({{< ref "docs/ops/metrics" >}}#dashboard-integration).


{{< top >}}
