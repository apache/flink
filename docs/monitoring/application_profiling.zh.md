---
title: "应用程序分析"
nav-parent_id: monitoring
nav-pos: 15
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

<a name="overview-of-custom-logging-with-apache-flink"></a>

## Apache Flink 自定义日志概述

每个独立的 JobManager，TaskManager，HistoryServer，ZooKeeper 守护进程都将 `stdout` 和 `stderr` 重定向到名称后缀为 `.out` 的文件，并将其内部的日志记录写入到 `.log` 后缀的文件。用户可以在 `env.java.opts`，`env.java.opts.jobmanager`，`env.java.opts.taskmanager`，`env.java.opts.historyserver` 和 `env.java.opts.client` 配置项中配置 Java 选项（包括 log 相关的选项），同样也可以使用脚本变量 `FLINK_LOG_PREFIX` 定义日志文件，并将选项括在双引号中以供后期使用。日志文件将使用 `FLINK_LOG_PREFIX` 与默认的 `.out` 和 `.log` 后缀一起滚动。

<a name="profiling-with-java-flight-recorder"></a>

## 使用 Java Flight Recorder 分析

Java Flight Recorder 是 Oracle JDK 内置的分析和事件收集框架。[Java Mission Control](http://www.oracle.com/technetwork/java/javaseproducts/mission-control/java-mission-control-1998576.html) 是一套先进的工具，可以对 Java Flight Recorder 收集的大量数据进行高效和详细的分析。配置示例：

{% highlight yaml %}
env.java.opts: "-XX:+UnlockCommercialFeatures -XX:+UnlockDiagnosticVMOptions -XX:+FlightRecorder -XX:+DebugNonSafepoints -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true,dumponexitpath=${FLINK_LOG_PREFIX}.jfr"
{% endhighlight %}

<a name="profiling-with-jitwatch"></a>

## 使用 JITWatch 分析

[JITWatch](https://github.com/AdoptOpenJDK/jitwatch/wiki) Java HotSpot JIT 编译器的日志分析器和可视化工具，用于检查内联决策，热方法，字节码和汇编。配置示例：

{% highlight yaml %}
env.java.opts: "-XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation -XX:LogFile=${FLINK_LOG_PREFIX}.jit -XX:+PrintAssembly"
{% endhighlight %}

<a name="analyzing-out-of-memory-problems"></a>

## 分析内存溢出问题（Out of Memory Problems）

如果你的 Flink 应用程序遇到 `OutOfMemoryExceptions` ，那么启用在内存溢出错误时堆转储是一个好主意。

{% highlight yaml %}
env.java.opts: "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${FLINK_LOG_PREFIX}.hprof"
{% endhighlight %}

堆转储将使你能够方便地分析用户代码中潜在的内存泄漏问题。如果内存泄漏是由 Flink 引起的，那么请联系[开发人员邮件列表](mailto:dev@flink.apache.org)。

<a name="analyzing-memory--garbage-collection-behaviour"></a>

## 分析内存和 Garbage Collection

内存使用和 garbage collection 会对你的应用程序产生巨大的影响。如果 GC 停顿时间过长，其影响力小到性能下降，大到集群全面瘫痪。如果你想更好地理解应用程序的内存和 GC 行为，可以在 `TaskManagers` 上启用内存日志记录。

{% highlight yaml %}
taskmanager.debug.memory.log: true
taskmanager.debug.memory.log-interval: 10000 // 10s interval
{% endhighlight %}

如果你想了解更详细的 GC 统计数据，可以通过以下方式激活 JVM 的 GC 日志记录：

{% highlight yaml %}
env.java.opts: "-Xloggc:${FLINK_LOG_PREFIX}.gc.log -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M -XX:+PrintPromotionFailure -XX:+PrintGCCause"
{% endhighlight %}

{% top %}
