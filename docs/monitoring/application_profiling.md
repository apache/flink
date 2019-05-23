---
title: "Application Profiling"
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

## Overview of Custom Logging with Apache Flink

Each standalone JobManager, TaskManager, HistoryServer, and ZooKeeper daemon redirects `stdout` and `stderr` to a file
with a `.out` filename suffix and writes internal logging to a file with a `.log` suffix. Java options configured by the
user in `env.java.opts`, `env.java.opts.jobmanager`, `env.java.opts.taskmanager` and `env.java.opts.historyserver` can likewise define log files with
use of the script variable `FLINK_LOG_PREFIX` and by enclosing the options in double quotes for late evaluation. Log files
using `FLINK_LOG_PREFIX` are rotated along with the default `.out` and `.log` files.

# Profiling with Java Flight Recorder

Java Flight Recorder is a profiling and event collection framework built into the Oracle JDK.
[Java Mission Control](http://www.oracle.com/technetwork/java/javaseproducts/mission-control/java-mission-control-1998576.html)
is an advanced set of tools that enables efficient and detailed analysis of the extensive of data collected by Java
Flight Recorder. Example configuration:

{% highlight yaml %}
env.java.opts: "-XX:+UnlockCommercialFeatures -XX:+UnlockDiagnosticVMOptions -XX:+FlightRecorder -XX:+DebugNonSafepoints -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true,dumponexitpath=${FLINK_LOG_PREFIX}.jfr"
{% endhighlight %}

# Profiling with JITWatch

[JITWatch](https://github.com/AdoptOpenJDK/jitwatch/wiki) is a log analyser and visualizer for the Java HotSpot JIT
compiler used to inspect inlining decisions, hot methods, bytecode, and assembly. Example configuration:

{% highlight yaml %}
env.java.opts: "-XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation -XX:LogFile=${FLINK_LOG_PREFIX}.jit -XX:+PrintAssembly"
{% endhighlight %}

{% top %}
