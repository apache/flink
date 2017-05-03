---
title: "Queryable State"
nav-parent_id: streaming
nav-pos: 61
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

In a nutshell, this feature allows users to query Flink's managed partitioned state
(see [Working with State]({{ site.baseurl }}/dev/stream/state.html)) from outside of
Flink. For some scenarios, queryable state thus eliminates the need for distributed
operations/transactions with external systems such as key-value stores which are often the
bottleneck in practice.

<div class="alert alert-warning">
  <strong>Attention:</strong> Queryable state accesses keyed state from a concurrent thread rather
  than synchronizing with the operator and potentially blocking its operation. Since any state
  backend using Java heap space, e.g. MemoryStateBackend or
  FsStateBackend, does not work with copies when retrieving values but instead directly
  references the stored values, read-modify-write patterns are unsafe and may cause the
  queryable state server to fail due to concurrent modifications.
  The RocksDBStateBackend is safe from these issues.
</div>

## Making State Queryable

In order to make state queryable, the queryable state server first needs to be enabled globally
by setting the `query.server.enable` configuration parameter to `true` (current default).
Then appropriate state needs to be made queryable by using either

* a `QueryableStateStream`, a convenience object which behaves like a sink and offers incoming values as
queryable state, or
* `StateDescriptor#setQueryable(String queryableStateName)`, which makes the keyed state of an
operator queryable.

The following sections explain the use of these two approaches.

### Queryable State Stream

A `KeyedStream` may offer its values as queryable state by using the following methods:

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

A call to these methods returns a `QueryableStateStream`, which cannot be further transformed and
currently only holds the name as well as the value and key serializer for the queryable state
stream. It is comparable to a sink, and cannot be followed by further transformations.

Internally a `QueryableStateStream` gets translated to an operator which uses all incoming
records to update the queryable state instance.
In a program like the following, all records of the keyed stream will be used to update the state
instance, either via `ValueState#update(value)` or `AppendingState#add(value)`, depending on
the chosen state variant:
{% highlight java %}
stream.keyBy(0).asQueryableState("query-name")
{% endhighlight %}
This acts like the Scala API's `flatMapWithState`.

### Managed Keyed State

Managed keyed state of an operator
(see [Using Managed Keyed State]({{ site.baseurl }}/dev/stream/state.html#using-managed-keyed-state))
can be made queryable by making the appropriate state descriptor queryable via
`StateDescriptor#setQueryable(String queryableStateName)`, as in the example below:
{% highlight java %}
ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
        new ValueStateDescriptor<>(
                "average", // the state name
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
descriptor.setQueryable("query-name"); // queryable state name
{% endhighlight %}
<div class="alert alert-info">
  <strong>Note:</strong> The `queryableStateName` parameter may be chosen arbitrarily and is only
  used for queries. It does not have to be identical to the state's own name.
</div>


## Querying State

The `QueryableStateClient` helper class may be used for queries against the `KvState` instances that
serve the state internally. It needs to be set up with a valid `JobManager` address and port and is
created as follows:

{% highlight java %}
final Configuration config = new Configuration();
config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, queryAddress);
config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, queryPort);

QueryableStateClient client = new QueryableStateClient(config);
{% endhighlight %}

The query method is this:

{% highlight java %}
Future<byte[]> getKvState(
    JobID jobID,
    String queryableStateName,
    int keyHashCode,
    byte[] serializedKeyAndNamespace)
{% endhighlight %}

A call to this method returns a `Future` eventually holding the serialized state value for the
queryable state instance identified by `queryableStateName` of the job with ID `jobID`. The
`keyHashCode` is the hash code of the key as returned by `Object.hashCode()` and the
`serializedKeyAndNamespace` is the serialized key and namespace.
<div class="alert alert-info">
  <strong>Note:</strong> The client is asynchronous and can be shared by multiple threads. It needs
  to be shutdown via <code>QueryableStateClient#shutdown()</code> when unused in order to free
  resources.
</div>

The current implementation is still pretty low-level in the sense that it only works with
serialized data both for providing the key/namespace and the returned results. It is the
responsibility of the user (or some follow-up utilities) to set up the serializers for this. The
nice thing about this is that the query services don't have to get into the business of worrying
about any class loading issues etc.

There are some serialization utils for key/namespace and value serialization included in
`KvStateRequestSerializer`.

### Example

The following example extends the `CountWindowAverage` example
(see [Using Managed Keyed State]({{ site.baseurl }}/dev/stream/state.html#using-managed-keyed-state))
by making it queryable and showing how to query this value:

{% highlight java %}
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private transient ValueState<Tuple2<Long /* count */, Long /* sum */>> sum;

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
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
        descriptor.setQueryable("query-name");
        sum = getRuntimeContext().getState(descriptor);
    }
}
{% endhighlight %}

Once used in a job, you can retrieve the job ID and then query any key's current state from this operator:

{% highlight java %}
final Configuration config = new Configuration();
config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, queryAddress);
config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, queryPort);

QueryableStateClient client = new QueryableStateClient(config);

final TypeSerializer<Long> keySerializer =
        TypeInformation.of(new TypeHint<Long>() {}).createSerializer(new ExecutionConfig());
final TypeSerializer<Tuple2<Long, Long>> valueSerializer =
        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}).createSerializer(new ExecutionConfig());

final byte[] serializedKey =
        KvStateRequestSerializer.serializeKeyAndNamespace(
                key, keySerializer,
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);

Future<byte[]> serializedResult =
        client.getKvState(jobId, "query-name", key.hashCode(), serializedKey);

// now wait for the result and return it
final FiniteDuration duration = new FiniteDuration(1, TimeUnit.SECONDS);
byte[] serializedValue = Await.result(serializedResult, duration);
Tuple2<Long, Long> value =
        KvStateRequestSerializer.deserializeValue(serializedValue, valueSerializer);
{% endhighlight %}

### Note for Scala Users

Please use the available Scala extensions when creating the `TypeSerializer` instances. Add the following import:

```scala
import org.apache.flink.streaming.api.scala._
```

Now you can create the type serializers as follows:

```scala
val keySerializer = createTypeInformation[Long]
  .createSerializer(new ExecutionConfig)
```

If you don't do this, you can run into mismatches between the serializers used in the Flink job and in your client code, because types like `scala.Long` cannot be caputured at runtime.

## Configuration

The following configuration parameters influence the behaviour of the queryable state server and client.
They are defined in `QueryableStateOptions`.

### Server
* `query.server.enable`: flag to indicate whether to start the queryable state server
* `query.server.port`: port to bind to the internal `KvStateServer` (0 => pick random available port)
* `query.server.network-threads`: number of network (event loop) threads for the `KvStateServer` (0 => #slots)
* `query.server.query-threads`: number of asynchronous query threads for the `KvStateServerHandler` (0 => #slots).

### Client (`QueryableStateClient`)
* `query.client.network-threads`: number of network (event loop) threads for the `KvStateClient` (0 => number of available cores)
* `query.client.lookup.num-retries`: number of retries on location lookup failures
* `query.client.lookup.retry-delay`: retry delay on location lookup failures (millis)

## Limitations

* The queryable state life-cycle is bound to the life-cycle of the job, e.g. tasks register
queryable state on startup and unregister it on disposal. In future versions, it is desirable to
decouple this in order to allow queries after a task finishes, and to speed up recovery via state
replication.
* Notifications about available KvState happen via a simple tell. In the future this should be improved to be
more robust with asks and acknowledgements.
* The server and client keep track of statistics for queries. These are currently disabled by
default as they would not be exposed anywhere. As soon as there is better support to publish these
numbers via the Metrics system, we should enable the stats.
