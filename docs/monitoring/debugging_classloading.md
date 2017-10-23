---
title: "Debugging Classloading"
nav-parent_id: monitoring
nav-pos: 14
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

## Overview of Classloading in Flink

When running Flink applications, the JVM will load various classes over time.
These classes can be divided into two domains:

  - The **Flink Framework** domain: This includes all code in the `/lib` directory in the Flink directory.
    By default these are the classes of Apache Flink and its core dependencies.

  - The **User Code** domain: These are all classes that are included in the JAR file submitted via the CLI or web interface.
    That includes the job's classes, and all libraries and connectors that are put into the uber JAR.


The class loading behaves slightly different for various Flink setups:

**Standalone**

When starting a Flink cluster, the JobManagers and TaskManagers are started with the Flink framework classes in the
classpath. The classes from all jobs that are submitted against the cluster are loaded *dynamically*.

**YARN**

YARN classloading differs between single job deployments and sessions:

  - When submitting a Flink job directly to YARN (via `bin/flink run -m yarn-cluster ...`), dedicated TaskManagers and
    JobManagers are started for that job. Those JVMs have both Flink framework classes and user code classes in their classpath.
    That means that there is *no dynamic classloading* involved in that case.

  - When starting a YARN session, the JobManagers and TaskManagers are started with the Flink framework classes in the
    classpath. The classes from all jobs that are submitted against the session are loaded dynamically.

**Mesos**

Mesos setups following [this documentation](../ops/deployment/mesos.html) currently behave very much like the a
YARN session: The TaskManager and JobManager processes are started with the Flink framework classes in classpath, job
classes are loaded dynamically when the jobs are submitted.

## Configuring ClassLoader Resolution Order

Flink uses a hierarchy of ClassLoaders for loading classes from the user-code jar(s). The user-code
ClassLoader has a reference to the parent ClassLoader, which is the default Java ClassLoader in most
cases. By default, Java ClassLoaders will first look for classes in the parent ClassLoader and then in
the child ClassLoader for cases where we have a hierarchy of ClassLoaders. This is problematic if you
have in your user jar a version of a library that conflicts with a version that comes with Flink. You can
change this behaviour by configuring the ClassLoader resolution order via
`classloader.resolve-order: child-first` in the Flink config. However, Flink classes will still
be resolved through the parent ClassLoader first, although you can also configure this via
`classloader.parent-first-patterns` (see [config](../ops/config.html))


## Avoiding Dynamic Classloading

All components (JobManger, TaskManager, Client, ApplicationMaster, ...) log their classpath setting on startup.
They can be found as part of the environment information at the beginning of the log.

When running a setup where the Flink JobManager and TaskManagers are exclusive to one particular job, one can put JAR files
directly into the `/lib` folder to make sure they are part of the classpath and not loaded dynamically.

It usually works to put the job's JAR file into the `/lib` directory. The JAR will be part of both the classpath
(the *AppClassLoader*) and the dynamic class loader (*FlinkUserCodeClassLoader*).
Because the AppClassLoader is the parent of the FlinkUserCodeClassLoader (and Java loads parent-first, by default), this should
result in classes being loaded only once.

For setups where the job's JAR file cannot be put to the `/lib` folder (for example because the setup is a session that is
used by multiple jobs), it may still be possible to put common libraries to the `/lib` folder, and avoid dynamic class loading
for those.


## Manual Classloading in the Job

In some cases, a transformation function, source, or sink needs to manually load classes (dynamically via reflection).
To do that, it needs the classloader that has access to the job's classes.

In that case, the functions (or sources or sinks) can be made a `RichFunction` (for example `RichMapFunction` or `RichWindowFunction`)
and access the user code class loader via `getRuntimeContext().getUserCodeClassLoader()`.


## X cannot be cast to X exceptions

When you see an exception in the style `com.foo.X cannot be cast to com.foo.X`, it means that multiple versions of the class
`com.foo.X` have been loaded by different class loaders, and types of that class are attempted to be assigned to each other.

The reason is in most cases that an object of the `com.foo.X` class loaded from a previous execution attempt is still cached somewhere,
and picked up by a restarted task/operator that reloaded the code. Note that this is again only possible in deployments that use
dynamic class loading.

Common causes of cached object instances:

  - When using *Apache Avro*: The *SpecificDatumReader* caches instances of records. Avoid using `SpecificData.INSTANCE`. See also
    [this discussion](http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/How-to-get-help-on-ClassCastException-when-re-submitting-a-job-tp10972p11133.html)

  - Using certain serialization frameworks for cloning objects (such as *Apache Avro*)

  - Interning objects (for example via Guava's Interners)


## Unloading of Dynamically Loaded Classes

All scenarios that involve dynamic class loading (i.e., standalone, sessions, mesos, ...) rely on classes being *unloaded* again.
Class unloading means that the Garbage Collector finds that no objects from a class exist and more, and thus removes the class
(the code, static variable, metadata, etc).

Whenever a TaskManager starts (or restarts) a task, it will load that specific task's code. Unless classes can be unloaded, this will
become a memory leak, as new versions of classes are loaded and the total number of loaded classes accumulates over time. This
typically manifests itself though a **OutOfMemoryError: PermGen**.

Common causes for class leaks and suggested fixes:

  - *Lingering Threads*: Make sure the application functions/sources/sinks shuts down all threads. Lingering threads cost resources themselves and
    additionally typically hold references to (user code) objects, preventing garbage collection and unloading of the classes.

  - *Interners*: Avoid caching objects in special structures that live beyond the lifetime of the functions/sources/sinks. Examples are Guava's
    interners, or Avro's class/object caches in the serializers.


## Resolving Dependency Conflicts with Flink using the maven-shade-plugin.

Apache Flink loads many classes by default into its classpath. If a user uses a different version of a library that Flink is using, often `IllegalAccessExceptions` or `NoSuchMethodError` are the result.

Through Hadoop, Flink for example depends on the `aws-sdk` library or on `protobuf-java`. If your user code is using these libraries and you run into issues we recommend relocating the dependency in your user code jar.

Apache Maven offers the [maven-shade-plugin](https://maven.apache.org/plugins/maven-shade-plugin/), which allows one to change the package of a class *after* compiling it (so the code you are writing is not affected by the shading). For example if you have the `com.amazonaws` packages from the aws sdk in your user code jar, the shade plugin would relocate them into the `org.myorg.shaded.com.amazonaws` package, so that your code is calling your aws sdk version.

This documentation page explains [relocating classes using the shade plugin](https://maven.apache.org/plugins/maven-shade-plugin/examples/class-relocation.html).


Note that some of Flink's dependencies, such as `guava` are shaded away by the maintainers of Flink, so users usually don't have to worry about it.

{% top %}
