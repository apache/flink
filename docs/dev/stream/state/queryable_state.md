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
  <strong>Note:</strong> The client APIs for queryable state are currently in an evolving state and
  there are <strong>no guarantees</strong> made about stability of the provided interfaces. It is
  likely that there will be breaking API changes on the client side in the upcoming Flink versions.
</div>

In a nutshell, this feature exposes Flink's managed keyed (partitioned) state
(see [Working with State]({{ site.baseurl }}/dev/stream/state/state.html)) to the outside world and 
allows the user to query a job's state from outside Flink. For some scenarios, queryable state 
eliminates the need for distributed operations/transactions with external systems such as key-value 
stores which are often the bottleneck in practice. In addition, this feature may be particularly 
useful for debugging purposes.

<div class="alert alert-warning">
  <strong>Attention:</strong> When querying a state object, that object is accessed from a concurrent 
  thread without any synchronization or copying. This is a design choice, as any of the above would lead
  to increased job latency, which we wanted to avoid. Since any state backend using Java heap space, 
  <i>e.g.</i> <code>MemoryStateBackend</code> or <code>FsStateBackend</code>, does not work 
  with copies when retrieving values but instead directly references the stored values, read-modify-write 
  patterns are unsafe and may cause the queryable state server to fail due to concurrent modifications.
  The <code>RocksDBStateBackend</code> is safe from these issues.
</div>

## Architecture

Before showing how to use the Queryable State, it is useful to briefly describe the entities that compose it.
The Queryable State feature consists of three main entities:

 1. the `QueryableStateClient`, which (potentially) runs outside the Flink cluster and submits the user queries, 
 2. the `QueryableStateClientProxy`, which runs on each `TaskManager` (*i.e.* inside the Flink cluster) and is responsible 
 for receiving the client's queries, fetching the requested state from the responsible Task Manager on his behalf, and 
 returning it to the client, and 
 3. the `QueryableStateServer` which runs on each `TaskManager` and is responsible for serving the locally stored state.
 
The client connects to one of the proxies and sends a request for the state associated with a specific 
key, `k`. As stated in [Working with State]({{ site.baseurl }}/dev/stream/state/state.html), keyed state is organized in 
*Key Groups*, and each `TaskManager` is assigned a number of these key groups. To discover which `TaskManager` is 
responsible for the key group holding `k`, the proxy will ask the `JobManager`. Based on the answer, the proxy will 
then query the `QueryableStateServer` running on that `TaskManager` for the state associated with `k`, and forward the
response back to the client.

## Activating Queryable State

To enable queryable state on your Flink cluster, you need to do the following:

 1. copy the `flink-queryable-state-runtime{{ site.scala_version_suffix }}-{{site.version }}.jar` 
from the `opt/` folder of your [Flink distribution](https://flink.apache.org/downloads.html "Apache Flink: Downloads"), 
to the `lib/` folder.
 2. set the property `queryable-state.enable` to `true`. See the [Configuration]({{ site.baseurl }}/ops/config.html#queryable-state) documentation for details and additional parameters.

To verify that your cluster is running with queryable state enabled, check the logs of any 
task manager for the line: `"Started the Queryable State Proxy Server @ ..."`.

## Making State Queryable

Now that you have activated queryable state on your cluster, it is time to see how to use it. In order for a state to 
be visible to the outside world, it needs to be explicitly made queryable by using:

* either a `QueryableStateStream`, a convenience object which acts as a sink and offers its incoming values as queryable
state, or
* the `stateDescriptor.setQueryable(String queryableStateName)` method, which makes the keyed state represented by the
 state descriptor, queryable.

The following sections explain the use of these two approaches.

### Queryable State Stream

Calling `.asQueryableState(stateName, stateDescriptor)` on a `KeyedStream` returns a `QueryableStateStream` which offers
its values as queryable state. Depending on the type of state, there are the following variants of the `asQueryableState()`
method:

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
  <strong>Note:</strong> There is no queryable <code>ListState</code> sink as it would result in an ever-growing
  list which may not be cleaned up and thus will eventually consume too much memory.
</div>

The returned `QueryableStateStream` can be seen as a sink and **cannot** be further transformed. Internally, a 
`QueryableStateStream` gets translated to an operator which uses all incoming records to update the queryable state 
instance. The updating logic is implied by the type of the `StateDescriptor` provided in the `asQueryableState` call. 
In a program like the following, all records of the keyed stream will be used to update the state instance via the 
`ValueState.update(value)`:

{% highlight java %}
stream.keyBy(0).asQueryableState("query-name")
{% endhighlight %}

This acts like the Scala API's `flatMapWithState`.

### Managed Keyed State

Managed keyed state of an operator
(see [Using Managed Keyed State]({{ site.baseurl }}/dev/stream/state/state.html#using-managed-keyed-state))
can be made queryable by making the appropriate state descriptor queryable via
`StateDescriptor.setQueryable(String queryableStateName)`, as in the example below:
{% highlight java %}
ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
        new ValueStateDescriptor<>(
                "average", // the state name
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})); // type information
descriptor.setQueryable("query-name"); // queryable state name
{% endhighlight %}

<div class="alert alert-info">
  <strong>Note:</strong> The <code>queryableStateName</code> parameter may be chosen arbitrarily and is only
  used for queries. It does not have to be identical to the state's own name.
</div>

This variant has no limitations as to which type of state can be made queryable. This means that this can be used for 
any `ValueState`, `ReduceState`, `ListState`, `MapState`, `AggregatingState`, and the currently deprecated `FoldingState`.

## Querying State

So far, you have set up your cluster to run with queryable state and you have declared (some of) your state as
queryable. Now it is time to see how to query this state. 

For this you can use the `QueryableStateClient` helper class. This is available in the `flink-queryable-state-client` 
jar which must be explicitly included as a dependency in the `pom.xml` of your project along with `flink-core`, as shown below:

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

For more on this, you can check how to [set up a Flink program]({{ site.baseurl }}/dev/project-configuration.html).

The `QueryableStateClient` will submit your query to the internal proxy, which will then process your query and return 
the final result. The only requirement to initialize the client is to provide a valid `TaskManager` hostname (remember 
that there is a queryable state proxy running on each task manager) and the port where the proxy listens. More on how 
to configure the proxy and state server port(s) in the [Configuration Section](#configuration).

{% highlight java %}
QueryableStateClient client = new QueryableStateClient(tmHostname, proxyPort);
{% endhighlight %}

With the client ready, to query a state of type `V`, associated with a key of type `K`, you can use the method:

{% highlight java %}
CompletableFuture<S> getKvState(
    JobID jobId,
    String queryableStateName,
    K key,
    TypeInformation<K> keyTypeInfo,
    StateDescriptor<S, V> stateDescriptor)
{% endhighlight %}

The above returns a `CompletableFuture` eventually holding the state value for the queryable state instance identified 
by `queryableStateName` of the job with ID `jobID`. The `key` is the key whose state you are interested in and the 
`keyTypeInfo` will tell Flink how to serialize/deserialize it. Finally, the `stateDescriptor` contains the necessary 
information about the requested state, namely its type (`Value`, `Reduce`, etc) and the necessary information on how 
to serialize/deserialize it.

The careful reader will notice that the returned future contains a value of type `S`, *i.e.* a `State` object containing
the actual value. This can be any of the state types supported by Flink: `ValueState`, `ReduceState`, `ListState`, `MapState`,
`AggregatingState`, and the currently deprecated `FoldingState`. 

<div class="alert alert-info">
  <strong>Note:</strong> These state objects do not allow modifications to the contained state. You can use them to get 
  the actual value of the state, <i>e.g.</i> using <code>valueState.get()</code>, or iterate over
  the contained <code><K, V></code> entries, <i>e.g.</i> using the <code>mapState.entries()</code>, but you cannot 
  modify them. As an example, calling the <code>add()</code> method on a returned list state will throw an 
  <code>UnsupportedOperationException</code>.
</div>

<div class="alert alert-info">
  <strong>Note:</strong> The client is asynchronous and can be shared by multiple threads. It needs
  to be shutdown via <code>QueryableStateClient.shutdown()</code> when unused in order to free
  resources.
</div>

### Example

The following example extends the `CountWindowAverage` example
(see [Using Managed Keyed State]({{ site.baseurl }}/dev/stream/state/state.html#using-managed-keyed-state))
by making it queryable and shows how to query this value:

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

Once used in a job, you can retrieve the job ID and then query any key's current state from this operator:

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

The following configuration parameters influence the behaviour of the queryable state server and client.
They are defined in `QueryableStateOptions`.

### State Server
* `queryable-state.server.ports`: the server port range of the queryable state server. This is useful to avoid port clashes if more 
   than 1 task managers run on the same machine. The specified range can be: a port: "9123", a range of ports: "50100-50200",
   or a list of ranges and or points: "50100-50200,50300-50400,51234". The default port is 9067.
* `queryable-state.server.network-threads`: number of network (event loop) threads receiving incoming requests for the state server (0 => #slots)
* `queryable-state.server.query-threads`: number of threads handling/serving incoming requests for the state server (0 => #slots).


### Proxy
* `queryable-state.proxy.ports`: the server port range of the queryable state proxy. This is useful to avoid port clashes if more 
  than 1 task managers run on the same machine. The specified range can be: a port: "9123", a range of ports: "50100-50200",
  or a list of ranges and or points: "50100-50200,50300-50400,51234". The default port is 9069.
* `queryable-state.proxy.network-threads`: number of network (event loop) threads receiving incoming requests for the client proxy (0 => #slots)
* `queryable-state.proxy.query-threads`: number of threads handling/serving incoming requests for the client proxy (0 => #slots).

## Limitations

* The queryable state life-cycle is bound to the life-cycle of the job, *e.g.* tasks register
queryable state on startup and unregister it on disposal. In future versions, it is desirable to
decouple this in order to allow queries after a task finishes, and to speed up recovery via state
replication.
* Notifications about available KvState happen via a simple tell. In the future this should be improved to be
more robust with asks and acknowledgements.
* The server and client keep track of statistics for queries. These are currently disabled by
default as they would not be exposed anywhere. As soon as there is better support to publish these
numbers via the Metrics system, we should enable the stats.

{% top %}
