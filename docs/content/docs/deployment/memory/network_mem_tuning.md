---
title: "Network Buffer Tuning"
weight: 100
type: docs
aliases:
  - /deployment/memory/network_mem_tuning.html
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

# Network memory tuning guide

## Overview

Each record in Flink is sent to the next subtask compounded with other records in a *network buffer*,
the smallest unit for communication between subtasks. In order to maintain consistent high throughput,
Flink uses *network buffer queues* (also known as *in-flight data*) on the input and output side of the transmission process. 

Each subtask has an input queue waiting to consume data and an output queue
waiting to send data to the next subtask. Having a larger amount of in-flight data means that Flink can provide higher and more resilient throughput in the pipeline.  This will, however, cause longer checkpoint times. 

Checkpoints in Flink can only finish once all the subtasks receive all of the injected checkpoint
barriers. In [aligned checkpoints]({{< ref "docs/concepts/stateful-stream-processing" >}}#checkpointing), those checkpoint barriers are traveling throughout the job graph along with
the network buffers. The larger the amount of in-flight data, the longer the checkpoint barrier propagation time. In [unaligned checkpoints]({{< ref "docs/concepts/stateful-stream-processing" >}}#unaligned-checkpointing), the larger the amount of in-flight data, the larger the checkpoint size will be because all of the captured in-flight data has to be persisted as part of the checkpoint.

## The Buffer Debloating Mechanism

Previously, the only way to configure the amount of in-flight data was to specify both the buffer amount and the buffer size. However, ideal values can be difficult to choose since they are different for every
deployment. The buffer debloating mechanism added in Flink 1.14 attempts to address this issue by automatically adjusting the amount of in-flight data to reasonable values.

The buffer debloating feature calculates the maximum possible throughput for the subtask (in the scenario that it is always busy) and adjusts the amount of in-flight data such that the consumption time of those in-flight data will be equal to the configured value.

The buffer debloat mechanism can be enabled by setting the property `taskmanager.network.memory.buffer-debloat.enabled` to `true`. 
The targeted time to consume the in-flight data can be configured by setting `taskmanager.network.memory.buffer-debloat.target` to a `duration`.
The default value of the debloat target should be good enough for most cases.

This feature uses past throughput data to predict the time required to consume the remaining
in-flight data. If the predictions are incorrect, the debloating mechanism can fail in one of two ways:
* There will not be enough buffered data to provide full throughput.
* There will be too many buffered in-flight data which will negatively affect the aligned checkpoint barriers propagation time or the unaligned checkpoint size.

If you have a varying load in your Job (i.e. sudden spikes of incoming records, periodically
firing windowed aggregations or joins), you might need to adjust the following settings:

* `taskmanager.network.memory.buffer-debloat.period` - This is the minimum time period between buffer size recalculation. The shorter the period, the faster the reaction time of the debloating mechanism but the higher the CPU overhead for the necessary calculations.

* `taskmanager.network.memory.buffer-debloat.samples` - This adjusts the number of samples over which throughput measurements are averaged out. The frequency of the collected samples can be adjusted via `taskmanager.network.memory.buffer-debloat.period`. The fewer the samples, the faster the reaction time of the debloating mechanism, but a higher chance of a sudden spike or drop of the throughput which can cause the buffer debloating mechanism to miscalculate the optimal amount of in-flight data.

* `taskmanager.network.memory.buffer-debloat.threshold-percentages` - An optimization for preventing frequent buffer size changes (i.e. if the new size is not much different compared to the old size).

Consult the [configuration]({{< ref "docs/deployment/config" >}}#full-taskmanageroptions) documentation for more details and additional parameters.

Here are [metrics]({{< ref "docs/ops/metrics" >}}#io) you can use to monitor the current buffer size:
* `estimatedTimeToConsumeBuffersMs` - total time to consume data from all input channels
* `debloatedBufferSize` - current buffer size

### Limitations

Currently, there are a few cases that are not handled automatically by the buffer debloating mechanism.

#### Large records

If your record size exceeds the [minimum memory segment size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-min-segment-size), buffer debloating can potentially shrink the buffer size so much, that the network stack will require more than one buffer to transfer a single record. This can have adverse effects on the throughput, without actually reducing the amount of in-flight data. 

#### Multiple inputs and unions

Currently, the throughput calculation and buffer debloating happen on the subtask level. 

If your subtask has multiple different inputs or it has a single but unioned input, buffer debloating can cause the input of the low throughput to have too much buffered in-flight data, while the input of the high throughput might have buffers that are too small to sustain that throughput. This might be particularly visible if the different inputs have vastly different throughputs. We recommend paying special attention to such subtasks when testing this feature.

#### Buffer size and number of buffers

Currently, buffer debloating only caps at the maximal used buffer size. The actual buffer size and the number of buffers remain unchanged. This means that the debloating mechanism cannot reduce the memory usage of your job. You would have to manually reduce either the amount or the size of the buffers. 

Furthermore, if you want to reduce the amount of buffered in-flight data below what buffer debloating currently allows, you might want to manually configure the number of buffers.

## Network buffer lifecycle
 
Flink has several local buffer pools - one for the output stream and one for each input gate. 
Each of those pools is limited to at most 

`#channels * taskmanager.network.memory.buffers-per-channel + taskmanager.network.memory.floating-buffers-per-gate`

The size of the buffer can be configured by setting `taskmanager.memory.segment-size`.

### Input network buffers

Buffers in the input channel are divided into exclusive and floating buffers.  Exclusive buffers can be used by only one particular channel.  A channel can request additional floating buffers from a buffer pool shared across all channels belonging to the given input gate. The remaining floating buffers are optional and are acquired only if there are enough resources available.

In the initialization phase:
- Flink will try to acquire the configured amount of exclusive buffers for each channel
- all exclusive buffers must be fulfilled or the job will fail with an exception
- a single floating buffer has to be allocated for Flink to be able to make progress

### Output network buffers

Unlike the input buffer pool, the output buffer pool has only one type of buffer which it shares among all subpartitions.

In order to avoid excessive data skew, the number of buffers for each subpartition is limited by the `taskmanager.network.memory.max-buffers-per-channel` setting.

Unlike the input buffer pool, the configured amount of exclusive buffers and floating buffers is only treated as recommended values. If there are not enough buffers available, Flink can make progress with only a single exclusive buffer per output subpartition and zero floating buffers.

## The number of in-flight buffers 

The default settings for exclusive buffers and floating buffers should be sufficient for the maximum throughput.  If the minimum of in-flight data needs to be set, the exclusive buffers can be set to `0` and the memory segment size can be decreased.

### Selecting the buffer size

The buffer collects records in order to optimize network overhead when sending the data portion to the next subtask. The next subtask should receive all parts of the record before consuming it. 

If the buffer size is too small, or the buffers are flushed too frequently (`execution.buffer-timeout` configuration parameter), this can lead to decreased throughput 
since the per-buffer overhead are significantly higher then per-record overheads in the Flink's runtime.

As a rule of thumb, we don't recommend thinking about increasing the buffer size, or the buffer timeout unless you can observe a network bottleneck in your real life workload
(downstream operator idling, upstream backpressured, output buffer queue is full, downstream input queue is empty).

If the buffer size is too large, this can lead to: 
- high memory usage
- huge checkpoint data (for unaligned checkpoints)
- long checkpoint time (for aligned checkpoints)
- inefficient use of allocated memory with a small `execution.buffer-timeout` because flushed buffers would only be sent partially filled 

### Selecting the buffer count

The number of buffers is configured by the `taskmanager.network.memory.buffers-per-channel` and `taskmanager.network.memory.floating-buffers-per-gate` settings. 

For best throughput, we recommend using the default values for the number of exclusive
and floating buffers. If the amount of in-flight data is causing issues, enabling
[buffer debloating]({{< ref "docs/deployment/memory/network_mem_tuning" >}}#the-buffer-debloating-mechanism) is recommended. 

You can tune the number of network buffers manually, but consider the following: 

1. You should adjust the number of buffers according to your expected throughput (in `bytes/second`).
Assigning credits and sending buffers takes some time (around two roundtrip messages between two nodes). The latency also depends on your network.

Using the buffer roundtrip time (around `1ms` in a healthy local network), [the buffer size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-segment-size), and the expected throughput, you can calculate the number of buffers required to sustain the throughput by using this formula:
```
number_of_buffers = expected_throughput * buffer_roundtrip / buffer_size
```
For example, with an expected throughput of `320MB/s`, roundtrip latency of `1ms`, and the default memory segment size, 10 is the number of actively used buffers needed to achieve the expected throughput:
```
number_of_buffers = 320MB/s * 1ms / 32KB = 10
```
2. The purpose of floating buffers is to handle data skew scenarios. Ideally, the number of floating buffers (default: 8) and the exclusive buffers (default: 2) that belong to that channel should be able to saturate the network throughput. But this is not always feasible or necessary. It is very rare that only a single channel among all the subtasks in the task manager is being used.

3. The purpose of exclusive buffers is to provide a fluent throughput. While one buffer is in transit, the other is being filled up. With high throughput setups, the number of exclusive buffers is the main factor that defines the amount of in-flight data Flink uses.

In the case of backpressure in low throughput setups, you should consider reducing the number of [exclusive buffers]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-buffers-per-channel).

## Summary

Memory configuration tuning for the network in Flink can be simplified by enabling the buffer debloating mechanism. You may have to tune it. 

If this does not work, you can disable the buffer debloating mechanism and manually configure the memory segment size and the number of buffers. For this second scenario, we recommend:
- using the default values for max throughput
- reducing the memory segment size and/or number of exclusive buffers to speed up checkpointing and reduce the memory consumption of the network stack

{{< top >}}
