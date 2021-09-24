---
title: Async Base
weight: 5
type: docs
aliases:
  - /dev/connectors/async_base.html
  - /apis/streaming/connectors/async_base.html
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

# Asynchronous Sink Base 

The basic functionalities of [sinks]({{< ref "docs/connectors/datastream/overview" >}}) are quite similar. They batch events according to user defined buffering hints, sign requests and send them to the respective endpoint, retry unsuccessful or throttled requests, and participate in checkpointing.

Such functionalities have been implemented in an abstract sink. Adding support for a new destination now just means creating a lightweight shim that only implements the specific interfaces of the destination using a client that supports async requests. Having a common abstraction will reduce the effort required to maintain all these individual sinks. It will also make it much easier and faster to create integrations with additional destinations. Moreover, improvements or bug fixes to the core of the sink will benefit all implementations that are based on it.

{{< hint warning >}}
**Attention** The sink is designed to participate in checkpointing to provide at-least once semantics, but it is limited to destinations that provide a client that supports async requests.
{{< /hint >}}

The design of the sink focuses on extensibility and a broad support of destinations. The core of the sink is kept generic and free of any connector specific dependencies.

You will find information on how to implement a sink based on this design in the rest of this document.

### Dependency

To use this connector, add the following dependency to your project:

{{< artifact flink-connector-base withScalaVersion >}}


## Public Interfaces

### Generic Types

`<InputT>` – elements of a DataStream that should be persisted in the destination

`<RequestEntryT>` – the payload of the element and additional metadata that is required to submit a single element to the destination


### Concrete Sink User Interface
```java
public interface ElementConverter<InputT, RequestEntryT> extends Serializable {
    RequestEntryT apply(InputT element, SinkWriter.Context context);
}
```
End users wanting to use this sink to persist events to a destination should provide a way to convert from an element in the DataStream to the payload type that contains all the additional metadata required to submit that element to the destination by the sink.

### Abstract Sink Implementer Interface

#### AsyncSinkWriter

```java
public abstract class AsyncSinkWriter<InputT, RequestEntryT extends Serializable>
        implements SinkWriter<InputT, Void, Collection<RequestEntryT>> {
    // ...
    protected abstract void submitRequestEntries
            (List<RequestEntryT> requestEntries, Consumer<Collection<RequestEntryT>> requestResult);
    // ...
}
```

This method should specify how a list of elements from the datastream may be persisted into the destination. Sink implementers of various datastore and data processing vendors may use their own clients in connecting to and persisting the requestEntries received by this method.

Should any elements fail to be persisted, they should be requeued back in the buffer for retry using `requestResult.accept(...list of failed entries...)`. However, retrying any element that is known to be faulty and consistently failing, will result in that element being requeued forever, therefore a sensible strategy for determining what should be retried is highly recommended.

```java
public abstract class AsyncSinkWriter<InputT, RequestEntryT extends Serializable>
        implements SinkWriter<InputT, Void, Collection<RequestEntryT>> {
    // ...
    protected abstract long getSizeInBytes(RequestEntryT requestEntry);
    // ...
}
```
The generic sink has a concept of size of elements in the buffer. This allows users to specify a byte size threshold beyond which elements will be flushed. However the sink implementer is best positioned to determine what is most sensible measure of size for each `RequestEntryT` is.


```java
public abstract class AsyncSinkWriter<InputT, RequestEntryT extends Serializable>
        implements SinkWriter<InputT, Void, Collection<RequestEntryT>> {
    // ...
    public AsyncSinkWriter(
            ElementConverter<InputT, RequestEntryT> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long flushOnBufferSizeInBytes,
            long maxTimeInBufferMS) { /* ... */ }
    // ...
}
```

There are 5 sink configuration settings that control the buffering, flushing and retry behaviour of the sink.

* `int maxBatchSize` - maximum number of elements that may be passed in the   list to submitRequestEntries to be written downstream.
* `int maxInFlightRequests` - maximum number of uncompleted calls to submitRequestEntries that the SinkWriter will allow at any given point. Once this point has reached, writes and callbacks to add elements to the buffer may block until one or more requests to submitRequestEntries completes.
* `int maxBufferedRequests` - the maximum buffer length. Callbacks to add elements to the buffer and calls to write will block if this length has been reached and will only unblock if elements from the buffer have been removed for flushing.
* `long flushOnBufferSizeInBytes` - a flush will be attempted if the most recent call to write introduces an element to the buffer such that the total size of the buffer is greater than or equal to this threshold value.
* `long maxTimeInBufferMS` - the maximum amount of time an element may remain in the buffer. In most cases elements are flushed as a result of the batch size (in bytes or number) being reached or during a snapshot. However, there are scenarios where an element may remain in the buffer forever or a long period of time. To mitigate this, a timer is constantly active in the buffer such that: while the buffer is not empty, it will flush every maxTimeInBufferMS milliseconds.


#### AsyncSinkBase

```java
public class MySink<InputT> extends AsyncSinkBase<InputT, RequestEntryT> {
    // ...
    @Override
    public SinkWriter<InputT, Void, Collection<PutRecordsRequestEntry>> createWriter
            (InitContext context, List<Collection<PutRecordsRequestEntry>> states) {
        return new MySinkWriter(context);
    }
    // ...
}
```
The `AsyncSinkBase` ties in both the committer and the writer. Since at-most once semantics are not being considered, the committers are returning empties. Sink implementers extending this would just need to return their implementation of the `AsyncSinkWriter` from `createWriter()` and their sink would be ready to go.

{{< top >}}
