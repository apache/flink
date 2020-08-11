---
title: "指标"
nav-parent_id: python_tableapi
nav-pos: 110
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

 PyFlink公开了一个指标系统，该指标系统允许收集指标并将其暴露给外部系统。 

* This will be replaced by the TOC
{:toc}

## 注册指标

您可以通过在`` open ``方法中调用`` function_context.get_metric_group() ``从[用户定义的函数]({{ site.baseurl }}/dev/table/python/python_udfs.html)访问度量系统。 
`` get_metric_group() ``方法返回一个`` MetricGroup ``对象，您可以在该对象上创建和注册新指标。 

### 指标类型

 PyFlink支持计数器`` Counters `` ， 量表`` Gauges `` ， 分布`` Distribution ``和仪表`` Meters `` 。 

#### 计数器 Counter

 `` Counter ``用于计数某物。当前值可以使用`` inc()/inc(n: int) ``或`` dec()/dec(n: int) ``递增或递减。
 您可以通过在`` MetricGroup ``上调用`` counter(name: str) ``来创建和注册`` Counter `` 。 

<div class="codetabs" markdown="1">
<div data-lang="python" markdown="1">
{% highlight python %}
from pyflink.table.udf import ScalarFunction

class MyUDF(ScalarFunction):

    def __init__(self):
        self.counter = None

    def open(self, function_context):
        self.counter = function_context.get_metric_group().counter("my_counter")

    def eval(self, i):
        self.counter.inc(i)
        return i

{% endhighlight %}
</div>

</div>

#### 量表

 `` Gauge ``可按需返回数值。您可以通过在MetricGroup上调用`` gauge(name: str, obj: Callable[[], int]) ``来注册一个仪表。 Callable对象将用于报告值。量表指标(Gauge metrics)仅限于仅整数值。 

<div class="codetabs" markdown="1">
<div data-lang="python" markdown="1">
{% highlight python %}
from pyflink.table.udf import ScalarFunction

class MyUDF(ScalarFunction):

    def __init__(self):
        self.length = 0

    def open(self, function_context):
        function_context.get_metric_group().gauge("my_gauge", lambda : self.length)

    def eval(self, i):
        self.length = i
        return i - 1
{% endhighlight %}
</div>

</div>

#### 分布（Distribution）

报告有关报告值分布的信息（总和，计数，最小，最大和平均值）的指标。可以使用`` update(n: int) ``该值。您可以通过在MetricGroup上调用`` distribution(name: str) ``来注册发布。分布指标(Distribution metrics)仅限于仅整数分布。 

<div class="codetabs" markdown="1">
<div data-lang="python" markdown="1">
{% highlight python %}
from pyflink.table.udf import ScalarFunction

class MyUDF(ScalarFunction):

    def __init__(self):
        self.distribution = None

    def open(self, function_context):
        self.distribution = function_context.get_metric_group().distribution("my_distribution")

    def eval(self, i):
        self.distribution.update(i)
        return i - 1
{% endhighlight %}
</div>

</div>

#### 仪表

仪表测量平均吞吐量。可以使用`` mark_event() ``函数注册事件的发生。可以使用mark_event(n: int)函数注册同时发生的多个事件。您可以通过在MetricGroup上调用`` meter(self, name: str, time_span_in_seconds: int = 60) ``来注册仪表。 time_span_in_seconds的默认值为60。 

<div class="codetabs" markdown="1">
<div data-lang="python" markdown="1">
{% highlight python %}
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
{% endhighlight %}
</div>

</div>

## 范围（Scope）

您可以参考Java指标文档以获取有关[范围定义]({{ site.baseurl }}/monitoring/metrics.html#Scope)的更多详细信息。 

### 用户范围（User Scope）

您可以通过调用`` MetricGroup.add_group(key: str, value: str = None) ``来定义用户范围。如果extra不为None，则创建一个新的键值MetricGroup对。密钥组被添加到该组的子组中，而值组被添加到密钥组的子组中。在这种情况下，将返回值组并定义一个用户变量。 

<div class="codetabs" markdown="1">
<div data-lang="python" markdown="1">
{% highlight python %}

function_context
    .get_metric_group()
    .add_group("my_metrics")
    .counter("my_counter")

function_context
    .get_metric_group()
    .add_group("my_metrics_key", "my_metrics_value")
    .counter("my_counter")

{% endhighlight %}
</div>

</div>

### 系统范围（System Scope）

您可以参考Java指标文档以获取有关[系统范围]({{ site.baseurl }}/monitoring/metrics.html#system-scope)的更多详细信息。 

### 所有变量列表

您可以参考Java指标文档以获取有关[“所有变量列表”的]({{ site.baseurl }}/monitoring/metrics.html#list-of-all-variables)更多详细信息。 

### 用户变量（User Variables）

您可以通过调用`` MetricGroup.addGroup(key: str, value: str = None) ``并指定value参数来定义用户变量。 

 **重要提示：**用户变量不能在以`scope format`中使用。 

<div class="codetabs" markdown="1">
<div data-lang="python" markdown="1">
{% highlight python %}
function_context
    .get_metric_group()
    .add_group("my_metrics_key", "my_metrics_value")
    .counter("my_counter")
{% endhighlight %}
</div>

</div>

##  PyFlink和Flink的共通部分

您可以参考Java指标文档，以获取以下各节的更多详细信息： 

*    [Reporter]({{ site.baseurl }}/monitoring/metrics.html#reporter) 。 
*    [系统指标]({{ site.baseurl }}/monitoring/metrics.html#system-metrics) 。 
*    [延迟跟踪]({{ site.baseurl }}/monitoring/metrics.html#latency-tracking) 。 
*    [REST API集成]({{ site.baseurl }}/monitoring/metrics.html#rest-api-integration) 。 
*    [仪表板集成]({{ site.baseurl }}/monitoring/metrics.html#dashboard-integration) 。 


{% top %}
