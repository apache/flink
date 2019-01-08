---
title: "File Systems"
nav-parent_id: ops
nav-pos: 12
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

This page provides details on setting up and configuring distributed file systems for use with Flink.

## Flink' File System support

Flink uses file systems both as a source and sink in streaming/batch applications, and as a target for checkpointing.
These file systems can for example be *Unix/Windows file systems*, *HDFS*, or even object stores like *S3*.

The file system used for a specific file is determined by the file URI's scheme. For example `file:///home/user/text.txt` refers to
a file in the local file system, while `hdfs://namenode:50010/data/user/text.txt` refers to a file in a specific HDFS cluster.

File systems are represented via the `org.apache.flink.core.fs.FileSystem` class, which captures the ways to access and modify
files and objects in that file system. FileSystem instances are instantiates once per process and then cached / pooled, to
avoid configuration overhead per stream creation, and to enforce certain constraints, like connection/stream limits.

### Built-in File Systems

Flink directly implements the following file systems:

  - **local**: This file system is used when the scheme is *"file://"*, and it represents the file system of the local machine, 
including any NFS or SAN that is mounted into that local file system.

  - **S3**: Flink directly provides file systems to talk to Amazon S3, registered under the scheme *"s3://"*.
There are two alternative implementations, `flink-s3-fs-presto` and `flink-s3-fs-hadoop`, based on code from the [Presto project](https://prestodb.io/)
and the [Hadoop Project](https://hadoop.apache.org/). Both implementations are self-contained with no dependency footprint.
To use those when using Flink as a library, add the respective maven dependency (`org.apache.flink:flink-s3-fs-presto:{{ site.version }}` or `org.apache.flink:flink-s3-fs-hadoop:{{ site.version }}`).
When starting a Flink application from the Flink binaries, copy or move the respective jar file from the `opt` folder to the `lib` folder.
See [AWS setup](deployment/aws.html) for details.

  - **MapR FS**: The MapR file system *"maprfs://"* is automatically available when the MapR libraries are in the classpath.
  
  - **OpenStack Swift FS**: Flink directly provides a file system to talk to the OpenStack Swift file system, registered under the scheme *"swift://"*. 
  The implementation `flink-swift-fs-hadoop` is based on the [Hadoop Project](https://hadoop.apache.org/) but is self-contained with no dependency footprint.
  To use it when using Flink as a library, add the respective maven dependency (`org.apache.flink:flink-swift-fs-hadoop:{{ site.version }}`
  When starting a Flink application from the Flink binaries, copy or move the respective jar file from the `opt` folder to the `lib` folder.

### HDFS and Hadoop File System support 

For a scheme where Flink does not implemented a file system itself, Flink will try to use Hadoop to instantiate a file system for the respective scheme.
All Hadoop file systems are automatically available once `flink-runtime` and the relevant Hadoop libraries are in classpath.

That way, Flink seamlessly supports all of Hadoop file systems, and all Hadoop-compatible file systems (HCFS), for example:

  - **hdfs**
  - **ftp**
  - **s3n** and **s3a**
  - **har**
  - ...


## Common File System configurations

The following configuration settings exist across different file systems.

#### Default File System

If paths to files do not explicitly specify a file system scheme (and authority), a default scheme (and authority) will be used.

{% highlight yaml %}
fs.default-scheme: <default-fs>
{% endhighlight %}

For example, if the default file system configured as `fs.default-scheme: hdfs://localhost:9000/`, then a file path of
`/user/hugo/in.txt` is interpreted as `hdfs://localhost:9000/user/hugo/in.txt`.

#### Connection limiting

You can limit the total number of connections that a file system can concurrently open. This is useful when the file system cannot handle a large number
of concurrent reads / writes or open connections at the same time.

For example, very small HDFS clusters with few RPC handlers can sometimes be overwhelmed by a large Flink job trying to build up many connections during a checkpoint.

To limit a specific file system's connections, add the following entries to the Flink configuration. The file system to be limited is identified by
its scheme.

{% highlight yaml %}
fs.<scheme>.limit.total: (number, 0/-1 mean no limit)
fs.<scheme>.limit.input: (number, 0/-1 mean no limit)
fs.<scheme>.limit.output: (number, 0/-1 mean no limit)
fs.<scheme>.limit.timeout: (milliseconds, 0 means infinite)
fs.<scheme>.limit.stream-timeout: (milliseconds, 0 means infinite)
{% endhighlight %}

You can limit the number if input/output connections (streams) separately (`fs.<scheme>.limit.input` and `fs.<scheme>.limit.output`), as well as impose a limit on
the total number of concurrent streams (`fs.<scheme>.limit.total`). If the file system tries to open more streams, the operation will block until some streams are closed.
If the opening of the stream takes longer than `fs.<scheme>.limit.timeout`, the stream opening will fail.

To prevent inactive streams from taking up the complete pool (preventing new connections to be opened), you can add an inactivity timeout for streams:
`fs.<scheme>.limit.stream-timeout`. If a stream does not read/write any bytes for at least that amount of time, it is forcibly closed.

These limits are enforced per TaskManager, so each TaskManager in a Flink application or cluster will open up to that number of connections.
In addition, the limits are also only enforced per FileSystem instance. Because File Systems are created per scheme and authority, different
authorities will have their own connection pool. For example `hdfs://myhdfs:50010/` and `hdfs://anotherhdfs:4399/` will have separate pools.


## Adding new File System Implementations

File system implementations are discovered by Flink through Java's service abstraction, making it easy to add additional file system implementations.

In order to add a new File System, the following steps are needed:

  - Add the File System implementation, which is a subclass of `org.apache.flink.core.fs.FileSystem`.
  - Add a factory that instantiates that file system and declares the scheme under which the FileSystem is registered. This must be a subclass of `org.apache.flink.core.fs.FileSystemFactory`.
  - Add a service entry. Create a file `META-INF/services/org.apache.flink.core.fs.FileSystemFactory` which contains the class name of your file system factory class.

See the [Java Service Loader docs](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) for more details on how service loaders work.

{% top %}
