---
title: "Application Profiling & Debugging"
nav-parent_id: debugging
nav-pos: 3
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

## Overview of Custom Logging with Apache Flink

Each standalone JobManager, TaskManager, HistoryServer, and ZooKeeper daemon redirects `stdout` and `stderr` to a file
with a `.out` filename suffix and writes internal logging to a file with a `.log` suffix. Java options configured by the
user in `env.java.opts`, `env.java.opts.jobmanager`, `env.java.opts.taskmanager`, `env.java.opts.historyserver` and 
`env.java.opts.client` can likewise define log files with
use of the script variable `FLINK_LOG_PREFIX` and by enclosing the options in double quotes for late evaluation. Log files
using `FLINK_LOG_PREFIX` are rotated along with the default `.out` and `.log` files.

## Profiling with Java Flight Recorder

Java Flight Recorder is a profiling and event collection framework built into the Oracle JDK.
[Java Mission Control](http://www.oracle.com/technetwork/java/javaseproducts/mission-control/java-mission-control-1998576.html)
is an advanced set of tools that enables efficient and detailed analysis of the extensive of data collected by Java
Flight Recorder. Example configuration:

{% highlight yaml %}
env.java.opts: "-XX:+UnlockCommercialFeatures -XX:+UnlockDiagnosticVMOptions -XX:+FlightRecorder -XX:+DebugNonSafepoints -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true,dumponexitpath=${FLINK_LOG_PREFIX}.jfr"
{% endhighlight %}

## Profiling with JITWatch

[JITWatch](https://github.com/AdoptOpenJDK/jitwatch/wiki) is a log analyser and visualizer for the Java HotSpot JIT
compiler used to inspect inlining decisions, hot methods, bytecode, and assembly. Example configuration:

{% highlight yaml %}
env.java.opts: "-XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation -XX:LogFile=${FLINK_LOG_PREFIX}.jit -XX:+PrintAssembly"
{% endhighlight %}

## Analyzing Out of Memory Problems

If you encounter `OutOfMemoryExceptions` with your Flink application, then it is a good idea to enable heap dumps on out of memory errors.

{% highlight yaml %}
env.java.opts: "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${FLINK_LOG_PREFIX}.hprof"
{% endhighlight %}

The heap dump will allow you to analyze potential memory leaks in your user code.
If the memory leak should be caused by Flink, then please reach out to the [dev mailing list](mailto:dev@flink.apache.org).

## Analyzing Memory & Garbage Collection Behaviour

Memory usage and garbage collection can have a profound impact on your application.
The effects can range from slight performance degradation to a complete cluster failure if the GC pauses are too long.
If you want to better understand the memory and GC behaviour of your application, then you can enable memory logging on the `TaskManagers`.

{% highlight yaml %}
taskmanager.debug.memory.log: true
taskmanager.debug.memory.log-interval: 10000 // 10s interval
{% endhighlight %}

If you are interested in more detailed GC statistics, then you can activate the JVM's GC logging via:

{% highlight yaml %}
env.java.opts: "-Xloggc:${FLINK_LOG_PREFIX}.gc.log -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M -XX:+PrintPromotionFailure -XX:+PrintGCCause"
{% endhighlight %}

{% top %}
