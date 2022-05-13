---
title: "执行配置"
weight: 11
type: docs
aliases:
  - /zh/dev/execution_configuration.html
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

<a name="execution-configuration"></a>

# 执行配置

`StreamExecutionEnvironment` 包含了 `ExecutionConfig`，它允许在运行时设置作业特定的配置值。要更改影响所有作业的默认值，请参阅[配置]({{< ref "docs/deployment/config" >}})。

{{< tabs "2c6dd8a9-a118-4cc4-afbd-a42b02432ad3" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
ExecutionConfig executionConfig = env.getConfig();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
var executionConfig = env.getConfig
```
{{< /tab >}}
{{< /tabs >}}

以下是可用的配置选项：（默认为粗体）

- `setClosureCleanerLevel()`。closure cleaner 的级别默认设置为 `ClosureCleanerLevel.RECURSIVE`。closure cleaner 删除 Flink 程序中对匿名 function 的调用类的不必要引用。禁用 closure cleaner 后，用户的匿名 function 可能正引用一些不可序列化的调用类。这将导致序列化器出现异常。可设置的值是：
`NONE`：完全禁用 closure cleaner ，`TOP_LEVEL`：只清理顶级类而不递归到字段中，`RECURSIVE`：递归清理所有字段。

- `getParallelism()` / `setParallelism(int parallelism)`。为作业设置默认的并行度。

- `getMaxParallelism()` / `setMaxParallelism(int parallelism)`。为作业设置默认的最大并行度。此设置决定最大并行度并指定动态缩放的上限。

- `getNumberOfExecutionRetries()` / `setNumberOfExecutionRetries(int numberOfExecutionRetries)`。设置失败任务重新执行的次数。值为零会有效地禁用容错。`-1` 表示使用系统默认值（在配置中定义）。该配置已弃用，请改用[重启策略]({{< ref "docs/ops/state/task_failure_recovery" >}}#restart-strategies) 。

- `getExecutionRetryDelay()` / `setExecutionRetryDelay(long executionRetryDelay)`。设置系统在作业失败后重新执行之前等待的延迟（以毫秒为单位）。在 TaskManagers 上成功停止所有任务后，开始计算延迟，一旦延迟过去，任务会被重新启动。此参数对于延迟重新执行的场景很有用，当尝试重新执行作业时，由于相同的问题，作业会立刻再次失败，该参数便于作业再次失败之前让某些超时相关的故障完全浮出水面（例如尚未完全超时的断开连接）。此参数仅在执行重试次数为一次或多次时有效。该配置已被弃用，请改用[重启策略]({{< ref "docs/ops/state/task_failure_recovery" >}}#restart-strategies) 。

- `getExecutionMode()` / `setExecutionMode()`。默认的执行模式是 PIPELINED。设置执行模式以执行程序。执行模式定义了数据交换是以批处理方式还是以流方式执行。

- `enableForceKryo()` / **`disableForceKryo`**。默认情况下不强制使用 Kryo。强制 GenericTypeInformation 对 POJO 使用 Kryo 序列化器，即使我们可以将它们作为 POJO 进行分析。在某些情况下，应该优先启用该配置。例如，当 Flink 的内部序列化器无法正确处理 POJO 时。

- `enableForceAvro()` / **`disableForceAvro()`**。默认情况下不强制使用 Avro。强制 Flink AvroTypeInfo 使用 Avro 序列化器而不是 Kryo 来序列化 Avro 的 POJO。

- `enableObjectReuse()` / **`disableObjectReuse()`**。默认情况下，Flink 中不重用对象。启用对象重用模式会指示运行时重用用户对象以获得更好的性能。请当心，当一个算子的用户代码 function 没有意识到这种行为时可能会导致bug。

- `getGlobalJobParameters()` / `setGlobalJobParameters()`。此方法允许用户将自定义对象设置为作业的全局配置。由于 `ExecutionConfig` 可在所有用户定义的 function 中访问，因此这是一种使配置在作业中全局可用的简单方法。

- `addDefaultKryoSerializer(Class<?> type, Serializer<?> serializer)`。为指定的类型注册 Kryo 序列化器实例。

- `addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass)`。为指定的类型注册 Kryo 序列化器的类。

- `registerTypeWithKryoSerializer(Class<?> type, Serializer<?> serializer)`。使用 Kryo 注册指定类型并为其指定序列化器。通过使用 Kryo 注册类型，该类型的序列化将更加高效。

- `registerKryoType(Class<?> type)`。如果类型最终被 Kryo 序列化，那么它将在 Kryo 中注册，以确保只有标记（整数 ID）被写入。如果一个类型没有在 Kryo 注册，它的全限定类名将在每个实例中被序列化，从而导致更高的 I/O 成本。

- `registerPojoType(Class<?> type)`。将指定的类型注册到序列化栈中。如果该类型最终被序列化为 POJO，那么该类型将注册到 POJO 序列化器中。如果该类型最终被 Kryo 序列化，那么它将在 Kryo 中注册，以确保只有标记被写入。如果一个类型没有在 Kryo 注册，它的全限定类名将在每个实例中被序列化，从而导致更高的I/O成本。

注意：用 `registerKryoType()` 注册的类型对 Flink 的 Kryo 序列化器实例来说是不可用的。

- `disableAutoTypeRegistration()`。自动类型注册在默认情况下是启用的。自动类型注册是将用户代码使用的所有类型（包括子类型）注册到 Kryo 和 POJO 序列化器。

- `setTaskCancellationInterval(long interval)`。设置尝试连续取消正在运行任务的等待时间间隔（以毫秒为单位）。当一个任务被取消时，会创建一个新的线程，如果任务线程在一定时间内没有终止，新线程就会定期调用任务线程上的 `interrupt()` 方法。这个参数是指连续调用 `interrupt()` 的时间间隔，默认设置为 **30000** 毫秒，或 **30秒** 。

通过 `getRuntimeContext()` 方法在 `Rich*` function 中访问到的 `RuntimeContext` 也允许在所有用户定义的 function 中访问 `ExecutionConfig`。

{{< top >}}
