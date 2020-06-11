---
title: "Queryable State"
nav-parent_id: streaming_state
nav-pos: 4
is_beta: true
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

* ToC
{:toc}

<div class="alert alert-warning">
  <strong>注意:</strong> 目前 querable state 的客户端 API 还在不断演进，<strong>不保证</strong>现有接口的稳定性。在后续的 Flink 版本中有可能发生 API 变化。
</div>

简而言之, 这个特性将 Flink 的 managed keyed (partitioned) state
(参考 [Working with State]({{ site.baseurl }}/zh/dev/stream/state/state.html)) 暴露给外部，从而用户可以在 Flink 外部查询作业 state。
在某些场景中，Queryable State 消除了对外部系统的分布式操作以及事务的需求，比如 KV 存储系统，而这些外部系统往往会成为瓶颈。除此之外，这个特性对于调试作业非常有用。

<div class="alert alert-warning">
  <strong>注意:</strong> 进行查询时，state 会在并发线程中被访问，但 state 不会进行同步和拷贝。这种设计是为了避免同步和拷贝带来的作业延时。对于使用 Java 堆内存的 state backend，
  <i>比如</i> <code>MemoryStateBackend</code> 或者 <code>FsStateBackend</code>，它们获取状态时不会进行拷贝，而是直接引用状态对象，所以对状态的 read-modify-write 是不安全的，并且可能会因为并发修改导致查询失败。但 <code>RocksDBStateBackend</code> 是安全的，不会遇到上述问题。
</div>

## 架构

在展示如何使用 Queryable State 之前，先简单描述一下该特性的组成部分，主要包括以下三部分:

 1. `QueryableStateClient`，默认运行在 Flink 集群外部，负责提交用户的查询请求；
 2. `QueryableStateClientProxy`，运行在每个 `TaskManager` 上(*即* Flink 集群内部)，负责接收客户端的查询请求，从所负责的 Task Manager 获取请求的 state，并返回给客户端；
 3. `QueryableStateServer`, 运行在 `TaskManager` 上，负责服务本地存储的 state。

客户端连接到一个代理，并发送请求获取特定 `k` 对应的 state。 如 [Working with State]({{ site.baseurl }}/zh/dev/stream/state/state.html) 所述，keyed state 按照
*Key Groups* 进行划分，每个 `TaskManager` 会分配其中的一些 key groups。代理会询问 `JobManager` 以找到 `k` 所属 key group 的 TaskManager。根据返回的结果, 代理将会向运行在 `TaskManager` 上的 `QueryableStateServer` 查询 `k` 对应的 state， 并将结果返回给客户端。

## 激活 Queryable State

为了在 Flink 集群上使用 queryable state，需要进行以下操作：

 1. 将 `flink-queryable-state-runtime{{ site.scala_version_suffix }}-{{site.version }}.jar`
从 [Flink distribution](https://flink.apache.org/downloads.html "Apache Flink: Downloads") 的 `opt/` 目录拷贝到 `lib/` 目录；
 2. 将参数 `queryable-state.enable` 设置为 `true`。详细信息以及其它配置可参考文档 [Configuration]({{ site.baseurl }}/zh/ops/config.html#queryable-state)。

为了验证集群的 queryable state 已经被激活，可以检查任意 task manager 的日志中是否包含 "Started the Queryable State Proxy Server @ ..."。

## 将 state 设置为可查询的

激活集群的 queryable state 功能后，还要将 state 设置为可查询的才能对外可见，可以通过以下两种方式进行设置：

* 创建 `QueryableStateStream`，它会作为一个 sink，并将输入数据转化为 queryable state；
* 通过 `stateDescriptor.setQueryable(String queryableStateName)` 将 state 描述符所表示的 keyed state 设置成可查询的。

接下来的部分将详细解释这两种方式。

### Queryable State Stream

在 `KeyedStream` 上调用 `.asQueryableState(stateName, stateDescriptor)` 将会返回一个 `QueryableStateStream`， 它会将流数据转化为 queryable state。
对应不同的 state 类型，`asQueryableState()` 有以下一些方法变体：

{% highlight java %}
// ValueState
QueryableStateStream asQueryableState(
    String queryableStateName,
    ValueStateDescriptor stateDescriptor)

// Shortcut for explicit ValueStateDescriptor variant
QueryableStateStream asQueryableState(String queryableStateName)

// FoldingState
QueryableStateStream asQueryableState(
    String queryableStateName,
    FoldingStateDescriptor stateDescriptor)

// ReducingState
QueryableStateStream asQueryableState(
    String queryableStateName,
    ReducingStateDescriptor stateDescriptor)
{% endhighlight %}


<div class="alert alert-info">
  <strong>注意:</strong> 没有可查询的 <code>ListState</code> sink，因为这种情况下 list 会不断增长，并且可能不会被清理，最终会消耗大量的内存。
</div>

返回的 `QueryableStateStream` 可以被视作一个sink，而且**不能再**被进一步转换。在内部实现上，一个 `QueryableStateStream` 被转换成一个 operator，使用输入的数据来更新 queryable state。state 如何更新是由 `asQueryableState` 提供的 `StateDescriptor` 来决定的。在下面的代码中, keyed stream 的所有数据将会通过 `ValueState.update(value)` 来更新状态：

{% highlight java %}
stream.keyBy(0).asQueryableState("query-name")
{% endhighlight %}

这个行为类似于 Scala API 中的 `flatMapWithState`。

### Managed Keyed State

operator 中的 Managed keyed state
(参考 [Using Managed Keyed State]({{ site.baseurl }}/zh/dev/stream/state/state.html#using-managed-keyed-state))
可以通过 `StateDescriptor.setQueryable(String queryableStateName)` 将 state descriptor 设置成可查询的，从而使 state 可查询，如下所示：

{% highlight java %}
ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
        new ValueStateDescriptor<>(
                "average", // the state name
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})); // type information
descriptor.setQueryable("query-name"); // queryable state name
{% endhighlight %}

<div class="alert alert-info">
  <strong>注意:</strong> 参数 <code>queryableStateName</code> 可以任意选取，并且只被用来进行查询，它可以和 state 的名称不同。
</div>

这种方式不会限制 state 类型，即任意的 `ValueState`、`ReduceState`、`ListState`、`MapState`、`AggregatingState` 以及已弃用的 `FoldingState` 
均可作为 queryable state。

## 查询 state

目前为止，你已经激活了集群的 queryable state 功能，并且将一些 state 设置成了可查询的，接下来将会展示如何进行查询。

为了进行查询，可以使用辅助类 `QueryableStateClient`，这个类位于 `flink-queryable-state-client` 的 jar 中，在项目的 `pom.xml` 需要显示添加对 `flink-queryable-state-client` 和 `flink-core` 的依赖, 如下所示：

<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-core</artifactId>
  <version>{{ site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-queryable-state-client-java</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}
</div>

关于依赖的更多信息, 可以参考如何 [配置 Flink 项目]({{ site.baseurl }}/zh/dev/project-configuration.html).

`QueryableStateClient` 将提交你的请求到内部代理，代理会处理请求并返回结果。客户端的初始化只需要提供一个有效的 `TaskManager` 主机名
(每个 task manager 上都运行着一个 queryable state 代理)，以及代理监听的端口号。关于如何配置代理以及端口号可以参考 [Configuration Section](#configuration).

{% highlight java %}
QueryableStateClient client = new QueryableStateClient(tmHostname, proxyPort);
{% endhighlight %}

客户端就绪后，为了查询类型为 `K` 的 key，以及类型为 `V` 的state，可以使用如下方法：

{% highlight java %}
CompletableFuture<S> getKvState(
    JobID jobId,
    String queryableStateName,
    K key,
    TypeInformation<K> keyTypeInfo,
    StateDescriptor<S, V> stateDescriptor)
{% endhighlight %}

该方法会返回一个最终将包含 state 的 queryable state 实例，该实例可通过 JobID 和 queryableStateName 识别。在方法参数中，`key` 用来指定所要查询的状态所属的 key。
`keyTypeInfo` 告诉 Flink 如何对 key 进行序列化和反序列化。`stateDescriptor` 包含了所请求 state 的必要信息，即 state 类型(`Value`，`Reduce` 等等)，
以及如何对其进行序列化和反序列。

细心的读者会注意到返回的 future 包含类型为 `S` 的值，*即*一个存储实际值的 `State` 对象。它可以是Flink支持的任何类型的 state：`ValueState`、`ReduceState`、
`ListState`、`MapState`、`AggregatingState` 以及弃用的 `FoldingState`。

<div class="alert alert-info">
  <strong>注意：</strong> 这些 state 对象不允许对其中的 state 进行修改。你可以通过 <code>valueState.get()</code> 获取实际的 state，
  或者通过 <code>mapState.entries()</code> 遍历所有 <code><K, V></code>，但是不能修改它们。举例来说，对返回的 list state 调用 <code>add()</code>
   方法将会导致 <code>UnsupportedOperationException</code>。
</div>

<div class="alert alert-info">
  <strong>注意:</strong> 客户端是异步的，并且可能被多个线程共享。客户端不再使用后需要通过 <code>QueryableStateClient.shutdown()</code>
   来终止，从而释放资源。
</div>

### 示例

下面的例子扩展自 `CountWindowAverage`
(参考 [Using Managed Keyed State]({{ site.baseurl }}/zh/dev/stream/state/state.html#using-managed-keyed-state))，
将其中的 state 设置成可查询的，并展示了如何进行查询：

{% highlight java %}
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private transient ValueState<Tuple2<Long, Long>> sum; // a tuple containing the count and the sum

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
        Tuple2<Long, Long> currentSum = sum.value();
        currentSum.f0 += 1;
        currentSum.f1 += input.f1;
        sum.update(currentSum);

        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})); // type information
        descriptor.setQueryable("query-name");
        sum = getRuntimeContext().getState(descriptor);
    }
}
{% endhighlight %}

上面的代码作为作业运行后，可以获取作业的 ID，然后可以通过下面的方式查询任何 key 下的 state。

{% highlight java %}
QueryableStateClient client = new QueryableStateClient(tmHostname, proxyPort);

// the state descriptor of the state to be fetched.
ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
        new ValueStateDescriptor<>(
          "average",
          TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));

CompletableFuture<ValueState<Tuple2<Long, Long>>> resultFuture =
        client.getKvState(jobId, "query-name", key, BasicTypeInfo.LONG_TYPE_INFO, descriptor);

// now handle the returned value
resultFuture.thenAccept(response -> {
        try {
            Tuple2<Long, Long> res = response.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
});
{% endhighlight %}

## Configuration

下面的配置会影响 queryable state 服务器端和客户端的行为，它们定义在 `QueryableStateOptions`。

### State Server
* `queryable-state.server.ports`: 服务器端口范围，如果同一台机器上运行了多个 task manager，可以避免端口冲突。指定的可以是一个具体的端口号，如 "9123"，
    可以是一个端口范围，如 "50100-50200"，或者可以是端口范围以及端口号的组合，如 "50100-50200,50300-50400,51234"。默认端口号是 9067。
* `queryable-state.server.network-threads`: 服务器端 network (event loop) thread 的数量，用来接收查询请求 (如果设置为0，则线程数为 slot 数)。
* `queryable-state.server.query-threads`: 服务器端处理查询请求的线程数 (如果设置为0，则线程数为 slot 数)。


### Proxy
* `queryable-state.proxy.ports`: 代理的服务端口范围。如果同一台机器上运行了多个 task manager，可以避免端口冲突。指定的可以是一个具体的端口号，如 "9123"，
    可以是一个端口范围，如"50100-50200"，或者可以是端口范围以及端口号的组合，如 "50100-50200,50300-50400,51234"。默认端口号是 9069。
* `queryable-state.proxy.network-threads`: 代理上 network (event loop) thread 的数量，用来接收查询请求 (如果设置为0，则线程数为 slot 数)。
* `queryable-state.proxy.query-threads`: 代理上处理查询请求的线程数 (如果设置为0，则线程数为 slot 数)。

## 限制

* queryable state 的生命周期受限于作业的生命周期，*比如* tasks 在启动时注册可查询状态，并在退出时注销。在后续版本中，希望能够将其解耦
从而允许 task 结束后依然能够查询 state，并且通过 state 备份来加速恢复。
* 目前是通过 tell 来通知可用的 KvState。将来会使用 asks 和 acknowledgements 来提升稳定性。
* 服务器端和客户端会记录请求的统计信息。因为统计信息目前不会暴露给外部，所以这个功能默认没有开启。如果将来支持通过 Metrics 系统发布这些数据，将开启统计功能。

{% top %}
