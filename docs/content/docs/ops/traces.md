---
title: "Traces"
weight: 6
type: docs
aliases:
  - /ops/traces.html
  - /apis/traces.html
  - /monitoring/traces.html
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

# Traces

Flink exposes a tracing system that allows gathering and exposing traces to external systems.

## Reporting traces

You can access the tracing system from any user function that extends [RichFunction]({{< ref "docs/dev/datastream/user_defined_functions" >}}#rich-functions) by calling `getRuntimeContext().getMetricGroup()`.
This method returns a `MetricGroup` object via which you can report a new single span trace.

### Reporting single Span


A `Span` represents something that happened in Flink at certain point of time, that will be reported to a `TraceReporter`.
To report a `Span` you can use the `MetricGroup#addSpan(SpanBuilder)` method.

Currently we don't support traces with multiple spans. Each `Span` is self-contained and represents things like a checkpoint or recovery.
{{< tabs "9612d275-bdda-4322-a01f-ae6da805e917" >}}
{{< tab "Java" >}}
```java
public class MyClass {
    void doSomething() {
        // (...)
        metricGroup.addSpan(
                Span.builder(MyClass.class, "SomeAction")
                        .setStartTsMillis(startTs) // Optional
                        .setEndTsMillis(endTs) // Optional
                        .setAttribute("foo", "bar");
    }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Currently reporting Spans from Python is not supported.
```
{{< /tab >}}
{{< /tabs >}}

## Reporter

For information on how to set up Flink's trace reporters please take a look at the [trace reporters documentation]({{< ref "docs/deployment/trace_reporters" >}}).

{{< top >}}
