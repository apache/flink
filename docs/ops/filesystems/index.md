---
title: "File Systems"
nav-id: filesystems
nav-parent_id: ops
nav-show_overview: true
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

Apache Flink uses file systems to consume and persistently store data, both for the results of applications and for fault tolerance and recovery.
These are some of most of the popular file systems, including *local*, *hadoop-compatible*, *Amazon S3*, *MapR FS*, *OpenStack Swift FS*, *Aliyun OSS* and *Azure Blob Storage*.

The file system used for a particular file is determined by its URI scheme.
For example, `file:///home/user/text.txt` refers to a file in the local file system, while `hdfs://namenode:50010/data/user/text.txt` is a file in a specific HDFS cluster.

File system instances are instantiated once per process and then cached/pooled, to avoid configuration overhead per stream creation and to enforce certain constraints, such as connection/stream limits.

* This will be replaced by the TOC
{:toc}

## Local File System

Flink has built-in support for the file system of the local machine, including any NFS or SAN drives mounted into that local file system.
It can be used by default without additional configuration. Local files are referenced with the *file://* URI scheme.

## Pluggable File Systems

The Apache Flink project supports the following file systems:

  - [**Amazon S3**](./s3.html) object storage is supported by two alternative implementations: `flink-s3-fs-presto` and `flink-s3-fs-hadoop`.
  Both implementations are self-contained with no dependency footprint.

  - **MapR FS** file system adapter is already supported in the main Flink distribution under the *maprfs://* URI scheme.
  You must provide the MapR libraries in the classpath (for example in `lib` directory).

  - **OpenStack Swift FS** is supported by `flink-swift-fs-hadoop` and registered under the *swift://* URI scheme.
  The implementation is based on the [Hadoop Project](https://hadoop.apache.org/) but is self-contained with no dependency footprint.
  To use it when using Flink as a library, add the respective maven dependency (`org.apache.flink:flink-swift-fs-hadoop:{{ site.version }}`).
  
  - **[Aliyun Object Storage Service](./oss.html)** is supported by `flink-oss-fs-hadoop` and registered under the *oss://* URI scheme.
  The implementation is based on the [Hadoop Project](https://hadoop.apache.org/) but is self-contained with no dependency footprint.

  - **[Azure Blob Storage](./azure.html)** is supported by `flink-azure-fs-hadoop` and registered under the *wasb(s)://* URI schemes.
  The implementation is based on the [Hadoop Project](https://hadoop.apache.org/) but is self-contained with no dependency footprint.

Except **MapR FS**, you can use any of them as plugins. 

To use a pluggable file systems, copy the corresponding JAR file from the `opt` directory to a directory under `plugins` directory
of your Flink distribution before starting Flink, e.g.

{% highlight bash %}
mkdir ./plugins/s3-fs-hadoop
cp ./opt/flink-s3-fs-hadoop-{{ site.version }}.jar ./plugins/s3-fs-hadoop/
{% endhighlight %}

<span class="label label-danger">Attention</span> The plugin mechanism for file systems was introduced in Flink version `1.9` to
support dedicated Java class loaders per plugin and to move away from the class shading mechanism.
You can still use the provided file systems (or your own implementations) via the old mechanism by copying the corresponding
JAR file into `lib` directory. However, **since 1.10, s3 plugins must be loaded through the plugin mechanism**; the old
way no longer works as these plugins are not shaded anymore (or more specifically the classes are not relocated since 1.10).

It's encouraged to use the plugin-based loading mechanism for file systems that support it. Loading file systems components from the `lib`
directory may be not supported in future Flink versions.

## Adding a new pluggable File System implementation

File systems are represented via the `org.apache.flink.core.fs.FileSystem` class, which captures the ways to access and modify files and objects in that file system.

To add a new file system:

  - Add the File System implementation, which is a subclass of `org.apache.flink.core.fs.FileSystem`.
  - Add a factory that instantiates that file system and declares the scheme under which the FileSystem is registered. This must be a subclass of `org.apache.flink.core.fs.FileSystemFactory`.
  - Add a service entry. Create a file `META-INF/services/org.apache.flink.core.fs.FileSystemFactory` which contains the class name of your file system factory class
  (see the [Java Service Loader docs](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) for more details).

During plugins discovery, the file system factory class will be loaded by a dedicated Java class loader to avoid class conflicts with other plugins and Flink components.
The same class loader should be used during file system instantiation and the file system operation calls.

<span class="label label-warning">Warning</span> In practice, it means you should avoid using `Thread.currentThread().getContextClassLoader()` class loader
in your implementation. 

## Hadoop File System (HDFS) and its other implementations

For all schemes where Flink cannot find a directly supported file system, it falls back to Hadoop.
All Hadoop file systems are automatically available when `flink-runtime` and the Hadoop libraries are on the classpath.
See also **[Hadoop Integration]({{ site.baseurl }}/ops/deployment/hadoop.html)**.

This way, Flink seamlessly supports all of Hadoop file systems implementing the `org.apache.hadoop.fs.FileSystem` interface,
and all Hadoop-compatible file systems (HCFS).

  - HDFS (tested)
  - [Google Cloud Storage Connector for Hadoop](https://cloud.google.com/hadoop/google-cloud-storage-connector) (tested)
  - [Alluxio](http://alluxio.org/) (tested, see configuration specifics below)
  - [XtreemFS](http://www.xtreemfs.org/) (tested)
  - FTP via [Hftp](http://hadoop.apache.org/docs/r1.2.1/hftp.html) (not tested)
  - HAR (not tested)
  - ...

The Hadoop configuration has to have an entry for the required file system implementation in the `core-site.xml` file.
See example for **[Alluxio]({{ site.baseurl }}/ops/filesystems/#alluxio)**.

We recommend using Flink's built-in file systems unless required otherwise. Using a Hadoop File System directly may be required,
for example, when using that file system for YARN's resource storage, via the `fs.defaultFS` configuration property in Hadoop's `core-site.xml`.

### Alluxio

For Alluxio support add the following entry into the `core-site.xml` file:

{% highlight xml %}
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
{% endhighlight %}

{% top %}
