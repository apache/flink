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

This page provides details on setting up and configuring different file systems for use with Flink.
We start by describing how to use and configure the different file systems that are supported by Flink
out-of-the-box, before describing the necessary steps in order to add support about other/custom file system
implementations.

## Flink's File System support

Flink uses file systems both as *sources* and *sinks* in streaming/batch applications and as a target for *checkpointing*.
These file systems can for example be *Unix/Windows file systems*, *HDFS*, or even object stores like *S3*.

The file system used for a specific file is determined by the file URI's scheme. For example `file:///home/user/text.txt` refers to
a file in the local file system, while `hdfs://namenode:50010/data/user/text.txt` refers to a file in a specific HDFS cluster.

File systems are represented via the `org.apache.flink.core.fs.FileSystem` class, which captures the ways to access and modify
files and objects in that file system. FileSystem instances are instantiated once per process and then cached / pooled, to
avoid configuration overhead per stream creation and to enforce certain constraints, such as connection/stream limits.

### Built-in File Systems

Flink ships with support for most of the popular file systems, namely *local*, *hadoop-compatible*, *S3*, *MapR FS*
and *OpenStack Swift FS*. Each of these is identified by the scheme included in the URI of the provide file path. 

Flink ships with implementations for the following file systems:

  - **local**: This file system is used when the scheme is *"file://"*, and it represents the file system of the local machine, 
including any NFS or SAN that is mounted into that local file system.

  - **S3**: Flink directly provides file systems to talk to Amazon S3. There are two alternative implementations, `flink-s3-fs-presto`
    and `flink-s3-fs-hadoop`. Both implementations are self-contained with no dependency footprint. There is no need to add Hadoop to
    the classpath to use them. Both internally use some Hadoop code, but "shade away" all classes to avoid any dependency conflicts.

    - `flink-s3-fs-presto`, registered under the scheme *"s3://"* and *"s3p://"*, is based on code from the [Presto project](https://prestodb.io/).
      You can configure it the same way you can [configure the Presto file system](https://prestodb.io/docs/0.185/connector/hive.html#amazon-s3-configuration).
      
    - `flink-s3-fs-hadoop`, registered under *"s3://"* and *"s3a://"*, based on code from the [Hadoop Project](https://hadoop.apache.org/).
      The file system can be [configured exactly like Hadoop's s3a](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#S3A).

    To use those file systems when using Flink as a library, add the respective maven dependency (`org.apache.flink:flink-s3-fs-presto:{{ site.version }}`
    or `org.apache.flink:flink-s3-fs-hadoop:{{ site.version }}`). When starting a Flink application from the Flink binaries, copy or move
    the respective jar file from the `opt` folder to the `lib` folder. See also [AWS setup](deployment/aws.html) for additional details.
    
    <span class="label label-danger">Attention</span>: As described above, both Hadoop and Presto "listen" to paths with scheme set to *"s3://"*. This is 
    convenient for switching between implementations (Hadoop or Presto), but it can lead to non-determinism when both
    implementations are required. This can happen when, for example, the job uses the [StreamingFileSink]({{ site.baseurl}}/dev/connectors/streamfile_sink.html) 
    which only supports Hadoop, but uses Presto for checkpointing. In this case, it is advised to use explicitly *"s3a://"* 
    as a scheme for the sink (Hadoop) and *"s3p://"* for checkpointing (Presto).
    
  - **MapR FS**: The MapR file system *"maprfs://"* is automatically available when the MapR libraries are in the classpath.
  
  - **OpenStack Swift FS**: Flink directly provides a file system to talk to the OpenStack Swift file system, registered under the scheme *"swift://"*. 
  The implementation `flink-swift-fs-hadoop` is based on the [Hadoop Project](https://hadoop.apache.org/) but is self-contained with no dependency footprint.
  To use it when using Flink as a library, add the respective maven dependency (`org.apache.flink:flink-swift-fs-hadoop:{{ site.version }}`
  When starting a Flink application from the Flink binaries, copy or move the respective jar file from the `opt` folder to the `lib` folder.

#### HDFS and Hadoop File System support 

For all schemes where it cannot find a directly supported file system, Flink will try to use Hadoop to instantiate a file system for the respective scheme.
All Hadoop file systems are automatically available once `flink-runtime` and the Hadoop libraries are in classpath.

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

## Entropy injection for S3 file systems

The bundled S3 file systems (`flink-s3-fs-presto` and `flink-s3-fs-hadoop`) support entropy injection. Entropy injection is
a technique to improve scalability of AWS S3 buckets through adding some random characters near the beginning of the key.

If entropy injection is activated, a configured substring in the paths will be replaced by random characters. For example, path
`s3://my-bucket/checkpoints/_entropy_/dashboard-job/` would be replaced by something like `s3://my-bucket/checkpoints/gf36ikvg/dashboard-job/`.

**Note that this only happens when the file creation passes the option to inject entropy!**, otherwise the file path will
simply remove the entropy key substring. See
[FileSystem.create(Path, WriteOption)](https://ci.apache.org/projects/flink/flink-docs-release-1.6/api/java/org/apache/flink/core/fs/FileSystem.html#create-org.apache.flink.core.fs.Path-org.apache.flink.core.fs.FileSystem.WriteOptions-)
for details.

*Note: The Flink runtime currently passes the option to inject entropy only to checkpoint data files.*
*All other files, including checkpoint metadata and external URI do not inject entropy, to keep checkpoint URIs predictable.*

To enable entropy injection, configure the *entropy key* and the *entropy length* parameters.

```
s3.entropy.key: _entropy_
s3.entropy.length: 4 (default)

```

The `s3.entropy.key` defines the string in paths that is replaced by the random characters. Paths that do not contain the entropy key are left unchanged.
If a file system operation does not pass the *"inject entropy"* write option, the entropy key substring is simply removed.
The `s3.entropy.length` defined the number of random alphanumeric characters to replace the entropy key with.

## Adding new File System Implementations

File system implementations are discovered by Flink through Java's service abstraction, making it easy to add additional file system implementations.

In order to add a new File System, the following steps are needed:

  - Add the File System implementation, which is a subclass of `org.apache.flink.core.fs.FileSystem`.
  - Add a factory that instantiates that file system and declares the scheme under which the FileSystem is registered. This must be a subclass of `org.apache.flink.core.fs.FileSystemFactory`.
  - Add a service entry. Create a file `META-INF/services/org.apache.flink.core.fs.FileSystemFactory` which contains the class name of your file system factory class.

See the [Java Service Loader docs](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) for more details on how service loaders work.

{% top %}
