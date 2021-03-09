---
title: "Monitoring Back Pressure"
weight: 3
type: docs
aliases:
  - /ops/monitoring/back_pressure.html
  - /internals/back_pressure_monitoring.html
  - /ops/monitoring/back_pressure
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

# Monitoring Back Pressure

Flink's web interface provides a tab to monitor the back pressure behaviour of running jobs.

## Back Pressure

If you see a **back pressure warning** (e.g. `High`) for a task, this means that it is producing data faster than the downstream operators can consume. Records in your job flow downstream (e.g. from sources to sinks) and back pressure is propagated in the opposite direction, up the stream.

Take a simple `Source -> Sink` job as an example. If you see a warning for `Source`, this means that `Sink` is consuming data slower than `Source` is producing. `Sink` is back pressuring the upstream operator `Source`.


## Sampling Back Pressure

Back pressure monitoring works by repeatedly taking back pressure samples of your running tasks. The JobManager triggers repeated calls to `Task.isBackPressured()` for the tasks of your job.

{{< img src="/fig/back_pressure_sampling.png" class="img-responsive" >}}
<!-- https://docs.google.com/drawings/d/1O5Az3Qq4fgvnISXuSf-MqBlsLDpPolNB7EQG7A3dcTk/edit?usp=sharing -->

Internally, back pressure is judged based on the availability of output buffers. If there is no available buffer (at least one) for output, then it indicates that there is back pressure for the task.

By default, the job manager triggers 100 samples every 50ms for each task in order to determine back pressure. The ratio you see in the web interface tells you how many of these samples were indicating back pressure, e.g. `0.01` indicates that only 1 in 100 was back pressured.

- **OK**: 0 <= Ratio <= 0.10
- **LOW**: 0.10 < Ratio <= 0.5
- **HIGH**: 0.5 < Ratio <= 1

In order to not overload the task managers with back pressure samples, the web interface refreshes samples only after 60 seconds.

## Configuration

You can configure the number of samples for the job manager with the following configuration keys:

- `web.backpressure.refresh-interval`: Time after which available stats are deprecated and need to be refreshed (DEFAULT: 60000, 1 min).
- `web.backpressure.num-samples`: Number of samples to take to determine back pressure (DEFAULT: 100).
- `web.backpressure.delay-between-samples`: Delay between samples to determine back pressure (DEFAULT: 50, 50 ms).


## Example

You can find the *Back Pressure* tab next to the job overview.

### Sampling In Progress

This means that the JobManager triggered a back pressure sample of the running tasks. With the default configuration, this takes about 5 seconds to complete.

Note that clicking the row, you trigger the sample for all subtasks of this operator.

{{< img src="/fig/back_pressure_sampling_in_progress.png" class="img-responsive" >}}

### Back Pressure Status

If you see status **OK** for the tasks, there is no indication of back pressure. **HIGH** on the other hand means that the tasks are back pressured.

{{< img src="/fig/back_pressure_sampling_ok.png" class="img-responsive" >}}

{{< img src="/fig/back_pressure_sampling_high.png" class="img-responsive" >}}

{{< top >}}
