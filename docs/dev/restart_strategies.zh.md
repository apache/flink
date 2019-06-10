---
title: "重启策略"
nav-parent_id: execution
nav-pos: 50
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

Flink 在作业发生故障时支持不同的重启策略。如果没有为作业定义重启策略，集群启动时就会遵循默认的重启策略。
如果提交作业时设置了重启策略，该策略将覆盖掉集群的默认策略。

* This will be replaced by the TOC
{:toc}

## 概述

通过 Flink 的配置文件 `flink-conf.yaml` 来设置默认的重启策略。配置参数 *restart-strategy* 定义了采取何种策略。如果没有启用 checkpoint，就采用“不重启”策略。如果启用了 checkpoint 且没有配置重启策略，那么就采用固定延时重启策略，此时最大尝试重启次数由 `Integer.MAX_VALUE` 参数设置。下表列出了可用的重启策略和与其对应的配置值。

每个重启策略都有自己的一组配置参数来控制其行为。
这些参数也在配置文件中设置。
后文的描述中会详细介绍每种重启策略的配置项。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 50%">重启策略</th>
      <th class="text-left">restart-strategy 配置值</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>固定延时重启策略</td>
        <td>fixed-delay</td>
    </tr>
    <tr>
        <td>故障率重启策略</td>
        <td>failure-rate</td>
    </tr>
    <tr>
        <td>不重启策略</td>
        <td>none</td>
    </tr>
  </tbody>
</table>

除了定义默认的重启策略以外，还可以为每个 Flink 作业单独定义重启策略。
这个重启策略通过在程序中的 `ExecutionEnvironment` 对象上调用 `setRestartStrategy` 方法来设置。
当然，对于 `StreamExecutionEnvironment` 也同样适用。

下例展示了如何给我们的作业设置固定延时重启策略。
如果发生故障，系统会重启作业 3 次，每两次连续的重启尝试之间等待 10 秒钟。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // 尝试重启的次数
  Time.of(10, TimeUnit.SECONDS) // 延时
));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // 尝试重启的次数
  Time.of(10, TimeUnit.SECONDS) // 延时
))
{% endhighlight %}
</div>
</div>


## 重启策略

以下部分详细描述重启策略的配置项。

### 固定延时重启策略

固定延时重启策略按照给定的次数尝试重启作业。
如果尝试超过了给定的最大次数，作业将最终失败。
在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。

通过在 `flink-conf.yaml` 中设置如下配置参数，默认启用此策略。

{% highlight yaml %}
restart-strategy: fixed-delay
{% endhighlight %}

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">配置参数</th>
      <th class="text-left" style="width: 40%">描述</th>
      <th class="text-left">默认配置值</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><code>restart-strategy.fixed-delay.attempts</code></td>
        <td>作业宣告失败之前 Flink 重试执行的最大次数</td>
        <td>启用 checkpoint 的话是 <code>Integer.MAX_VALUE</code>，否则是 1</td>
    </tr>
    <tr>
        <td><code>restart-strategy.fixed-delay.delay</code></td>
        <td>延时重试意味着执行遭遇故障后，并不立即重新启动，而是延后一段时间。当程序与外部系统有交互时延时重试可能会有所帮助，比如程序里有连接或者挂起的事务的话，在尝试重新执行之前应该等待连接或者挂起的事务超时。</td>
        <td>启用 checkpoint 的话是 10 秒，否则使用 <code>akka.ask.timeout</code> 的值</td>
    </tr>
  </tbody>
</table>

例如：

{% highlight yaml %}
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10 s
{% endhighlight %}

固定延迟重启策略也可以在程序中设置：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // 尝试重启的次数
  Time.of(10, TimeUnit.SECONDS) // 延时
));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // 尝试重启的次数
  Time.of(10, TimeUnit.SECONDS) // 延时
))
{% endhighlight %}
</div>
</div>


### 故障率重启策略

故障率重启策略在故障发生之后重启作业，但是当**故障率**（每个时间间隔发生故障的次数）超过设定的限制时，作业会最终失败。
在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。

通过在 `flink-conf.yaml` 中设置如下配置参数，默认启用此策略。

{% highlight yaml %}
restart-strategy: failure-rate
{% endhighlight %}

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">配置参数</th>
      <th class="text-left" style="width: 40%">描述</th>
      <th class="text-left">配置默认值</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><it>restart-strategy.failure-rate.max-failures-per-interval</it></td>
        <td>单个时间间隔内允许的最大重启次数</td>
        <td>1</td>
    </tr>
    <tr>
        <td><it>restart-strategy.failure-rate.failure-rate-interval</it></td>
        <td>测量故障率的时间间隔</td>
        <td>1 分钟</td>
    </tr>
    <tr>
        <td><it>restart-strategy.failure-rate.delay</it></td>
        <td>连续两次重启尝试之间的延时</td>
        <td><it>akka.ask.timeout</it></td>
    </tr>
  </tbody>
</table>

例如：

{% highlight yaml %}
restart-strategy.failure-rate.max-failures-per-interval: 3
restart-strategy.failure-rate.failure-rate-interval: 5 min
restart-strategy.failure-rate.delay: 10 s
{% endhighlight %}

故障率重启策略也可以在程序中设置：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.failureRateRestart(
  3, // 每个时间间隔的最大故障次数
  Time.of(5, TimeUnit.MINUTES), // 测量故障率的时间间隔
  Time.of(10, TimeUnit.SECONDS) // 延时
));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.failureRateRestart(
  3, // 每个时间间隔的最大故障次数
  Time.of(5, TimeUnit.MINUTES), // 测量故障率的时间间隔
  Time.of(10, TimeUnit.SECONDS) // 延时
))
{% endhighlight %}
</div>
</div>


### 不重启策略

作业直接失败，不尝试重启。

{% highlight yaml %}
restart-strategy: none
{% endhighlight %}

不重启策略也可以在程序中设置：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.noRestart());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.noRestart())
{% endhighlight %}
</div>
</div>

### 备用重启策略

使用群集定义的重启策略。
这对于启用了 checkpoint 的流处理程序很有帮助。
如果没有定义其他重启策略，默认选择固定延时重启策略。

{% top %}
