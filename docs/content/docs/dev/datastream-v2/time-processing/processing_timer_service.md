---
title: "Processing Timer Service"
weight: 8
type: docs
aliases:
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

{{< hint warning >}}
**Note:** DataStream API V2 is a new set of APIs, to gradually replace the original DataStream API. It is currently in the experimental stage and is not fully available for production.
{{< /hint >}}

# Processing Timer Service

The process timer service is a fundamental primitive of the Flink DataStream API, provided by Flink. 
It allows users to register timers for performing calculations at specific processing time points.

For a comprehensive explanation of processing time, please refer to the section on [Notions of Time: Event Time and Processing Time]({{< ref "docs/concepts/time" >}}#notions-of-time-event-time-and-processing-time).

In this section, we will introduce how to utilize the processing timer service within the Flink DataStream API.

## ProcessingTimerManager

The `ProcessingTimerManager` is the key component for utilizing the processing timer service. 
You can obtain it by calling `PartitionedContext#getProcessingTimeManager` within a `ProcessFunction`. 
Below is the interface for the `ProcessingTimerManager`:

```java
@Experimental
public interface ProcessingTimeManager {
    /**
     * Register a processing timer for this process function. `onProcessingTimer` method of this
     * function will be invoked as callback if the timer expires.
     *
     * @param timestamp to trigger timer callback.
     */
    void registerTimer(long timestamp);

    /**
     * Deletes the processing-time timer with the given trigger timestamp. This method has only an
     * effect if such a timer was previously registered and did not already expire.
     *
     * @param timestamp indicates the timestamp of the timer to delete.
     */
    void deleteTimer(long timestamp);

    /**
     * Get the current processing time.
     *
     * @return current processing time.
     */
    long currentTime();
}
```

As shown above, `ProcessingTimerManager` has three methods: `registerTimer`, `deleteTimer`, and `currentTime`.
When the target time of `registerTimer` arrives, Flink will call `ProcessFunction#onProcessingTimer`, 
and users should write their own calculation logic in `ProcessFunction#onProcessingTimer`.

It should be noted that:
1. for timers with the same target time, Flink will only retain one timer. 
This means that if multiple timers with the same target time are registered, `ProcessFunction#onProcessingTimer` will only be invoked once.
2. the `ProcessingTimerManager` can only be used in Keyed Partitioned Stream.

## Example
Here is an example of using the processing timer service:

```java
public class CustomProcessFunction implements OneInputStreamProcessFunction<String, String> {

    @Override
    public void processRecord(
            String record,
            Collector<String> output,
            PartitionedContext<String> ctx) throws Exception {
        // do some calculation as needed

        // register a timer with a target time of one minute after the current time
        long currentTime = ctx.getProcessingTimeManager().currentTime();
        ctx.getProcessingTimeManager().registerTimer(currentTime + Duration.ofMinutes(1L).toMillis());
    }

    @Override
    public void onProcessingTimer(
            long timestamp,
            Collector<String> output,
            PartitionedContext<String> ctx) {
        // do some calculation and output result as needed
    }
}
```

{{< top >}}
