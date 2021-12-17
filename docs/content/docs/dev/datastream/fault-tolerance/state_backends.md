---
title: "State Backends"
weight: 6
type: docs
aliases:
  - /dev/stream/state/state_backends.html
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

# State Backends

Flink provides different state backends that specify how and where state is stored.

State can be located on Java’s heap or off-heap. Depending on your state backend, Flink can also manage the state for the application, meaning Flink deals with the memory management (possibly spilling to disk if necessary) to allow applications to hold very large state. By default, the configuration file *flink-conf.yaml* determines the state backend for all Flink jobs.

However, the default state backend can be overridden on a per-job basis, as shown below.

For more information about the available state backends, their advantages, limitations, and configuration parameters see the corresponding section in [Deployment & Operations]({{< ref "docs/ops/state/state_backends" >}}).

{{< tabs "65b41d30-c7c8-4b6b-b31b-7ff99b4d341d" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(...);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setStateBackend(...)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_state_backend(...)
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
