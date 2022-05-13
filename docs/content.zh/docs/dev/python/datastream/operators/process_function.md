---
title: "Process Function"
weight: 4
type: docs
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

# Process Function

## ProcessFunction

The `ProcessFunction` is a low-level stream processing operation, giving access to the basic building blocks of
all (acyclic) streaming applications:

- events (stream elements)
- state (fault-tolerant, consistent, only on keyed stream)
- timers (event time and processing time, only on keyed stream)

The `ProcessFunction` can be thought of as a `FlatMapFunction` with access to keyed state and timers. It handles events
by being invoked for each event received in the input stream(s).

Please refer to [Process Function]({{< ref "docs/dev/datastream/operators/process_function" >}})
for more details about the concept and usage of `ProcessFunction`.

## Execution behavior of timer

Python user-defined functions are executed in a separate Python process from Flink's operators which run in a JVM,
the timer registration requests made in `ProcessFunction` will be sent to the Java operator asynchronously.
Once received timer registration requests, the Java operator will register it into the underlying timer service.

If the registered timer has already passed the current time (the current system time for processing time timer,
or the current watermark for event time), it will be triggered immediately.

Note that, due to the asynchronous processing characteristics, it may happen that the timer was triggered a little later than the actual time.
For example, a registered processing time timer of `10:00:00` may be actually processed at `10:00:05`.
