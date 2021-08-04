---
title: "指标"
weight: 111
type: docs
aliases:
  - /zh/dev/python/table-api-users-guide/metrics.html
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

PyFlink 支持指标系统，该指标系统允许收集指标并将其暴露给外部系统。

## 注册指标

您可以通过在[Python 用户自定义函数]({{< ref "docs/dev/python/table/udfs/python_udfs" >}})的 `open` 方法中调用 `function_context.get_metric_group()` 来访问指标系统。
`get_metric_group()` 方法返回一个 `MetricGroup` 对象，您可以在该对象上创建和注册新指标。

### 指标类型

PyFlink 支持计数器 `Counters`，量表 `Gauges` ，分布 `Distribution` 和仪表 `Meters`。

#### 计数器 Counter

`Counter` 用于计算某个东西的出现次数。可以通过 `inc()/inc(n: int)` 或 `dec()/dec(n: int)` 增加或减少当前值。
您可以通过在 `MetricGroup` 上调用 `counter(name: str)` 来创建和注册 `Counter`。

{{< tabs "1da2d7f9-6031-459e-ae6c-64ac34957ebc" >}}
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

#### 量表

`Gauge` 可按需返回数值。您可以通过在 `MetricGroup` 上调用 `gauge(name: str, obj: Callable[[], int])` 来注册一个量表。Callable 对象将用于汇报数值。量表指标(Gauge metrics)只能用于汇报整数值。

{{< tabs "fcede6d0-0d62-4b17-9689-5c76114844ad" >}}
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

#### 分布（Distribution）

`Distribution` 用于报告关于所报告值分布的信息（总和，计数，最小，最大和平均值）的指标。可以通过 `update(n: int)` 来更新当前值。您可以通过在 `MetricGroup` 上调用 `distribution(name: str)` 来注册该指标。分布指标(Distribution metrics)只能用于汇报整数指标。

{{< tabs "838f66cd-ba1e-41cf-8d05-d86bd8d9a177" >}}
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

#### 仪表

仪表用于汇报平均吞吐量。可以使用 `mark_event()` 函数来注册事件的发生，使用 `mark_event(n: int)` 函数来注册同时发生的多个事件。
您可以通过在 `MetricGroup` 上调用 `meter(self, name: str, time_span_in_seconds: int = 60)` 来注册仪表。time_span_in_seconds的默认值为60。

{{< tabs "b9def1c8-1669-4950-9854-fe281c234d1a" >}}
{{< tab "Python" >}}
```python
from pyflink.table.udf import ScalarFunction

class MyUDF(ScalarFunction):

    def __init__(self):
        self.meter = None

    def open(self, function_context):
        super().open(function_context)
        # 120秒内统计的平均每秒事件数，默认是60秒
        self.meter = function_context.get_metric_group().meter("my_meter", time_span_in_seconds=120)

    def eval(self, i):
        self.meter.mark_event(i)
        return i - 1
```
{{< /tab >}}
{{< /tabs >}}

## 范围（Scope）

您可以参考 Java 指标文档以获取有关[范围定义]({{< ref "docs/ops/metrics" >}}#Scope)的更多详细信息。

### 用户范围（User Scope）

您可以通过调用 `MetricGroup.add_group(key: str, value: str = None)` 来定义用户范围。如果 `value` 不为 `None`，则创建一个新的键值 `MetricGroup`对。
其中，键组被添加到该组的子组中，而值组又被添加到键组的子组中。在这种情况下，值组将作为结果返回，与此同时，创建一个用户变量。

{{< tabs "1223f3b0-b907-4191-a19c-6fe421d5c6b3" >}}
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

### 系统范围（System Scope）

您可以参考 Java 指标文档以获取有关[系统范围]({{< ref "docs/ops/metrics" >}}#system-scope)的更多详细信息。

### 所有变量列表

您可以参考 Java 指标文档以获取有关[“所有变量列表”的]({{< ref "docs/ops/metrics" >}}#list-of-all-variables)更多详细信息。

### 用户变量（User Variables）

您可以通过调用 `MetricGroup.addGroup(key: str, value: str = None)` 并指定 value 参数来定义用户变量。

**重要提示：**用户变量不能在以 `scope format` 中使用。

{{< tabs "6d0715c0-6c39-489a-b3f3-e9bf7d50c268" >}}
{{< tab "Python" >}}
```python
function_context
    .get_metric_group()
    .add_group("my_metrics_key", "my_metrics_value")
    .counter("my_counter")
```
{{< /tab >}}
{{< /tabs >}}

##  PyFlink 和 Flink 的共通部分

您可以参考 Java 的指标文档，以获取关于以下部分的更多详细信息：

*    [Reporter]({{< ref "docs/deployment/metric_reporters" >}}) 。
*    [系统指标]({{< ref "docs/ops/metrics" >}}#system-metrics) 。
*    [延迟跟踪]({{< ref "docs/ops/metrics" >}}#latency-tracking) 。
*    [REST API 集成]({{< ref "docs/ops/metrics" >}}#rest-api-integration) 。
*    [仪表板集成]({{< ref "docs/ops/metrics" >}}#dashboard-integration) 。


{{< top >}}
