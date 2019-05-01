---
title: "文件系统"
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

Apache Flink uses to consume and persistently store data, both for results of applications and for fault tolerance and recovery.
These file systems can be local such as *Unix*, distributed like *HDFS*, or even object stores such as *S3*.

The file system used for a particular file is determined by its URI scheme.
For example, `file:///home/user/text.txt` refers to a file in the local file system, while `hdfs://namenode:50010/data/user/text.txt` is a file in a specific HDFS cluster.

File system instances are instantiated once per process and then cached / pooled, to
avoid configuration overhead per stream creation and to enforce certain constraints, such as connection/stream limits.

* This will be replaced by the TOC
{:toc}

### Built-in File Systems

Flink ships with support for most of the popular file systems, including *local*, *hadoop-compatible*, *S3*, *MapR FS* and *OpenStack Swift FS*.
Each is identified by the scheme included in the URI of the provide file path. 

Flink ships with implementations for the following file systems:

  - **local**: This file system is used when the scheme is *"file://"*, and it represents the file system of the local machine, including any NFS or SAN that is mounted into that local file system.

  - **S3**: Flink directly provides file systems to talk to Amazon S3 with two alternative implementations, `flink-s3-fs-presto` and `flink-s3-fs-hadoop`. Both implementations are self-contained with no dependency footprint.
    
  - **MapR FS**: The MapR file system *"maprfs://"* is automatically available when the MapR libraries are in the classpath.
  
  - **OpenStack Swift FS**: Flink directly provides a file system to talk to the OpenStack Swift file system, registered under the scheme *"swift://"*. 
  The implementation `flink-swift-fs-hadoop` is based on the [Hadoop Project](https://hadoop.apache.org/) but is self-contained with no dependency footprint.
  To use it when using Flink as a library, add the respective maven dependency (`org.apache.flink:flink-swift-fs-hadoop:{{ site.version }}`
  When starting a Flink application from the Flink binaries, copy or move the respective jar file from the `opt` folder to the `lib` folder.

#### HDFS and Hadoop File System support 

For all schemes where Flink cannot find a directly supported file system, it will fall back to Hadoop.
All Hadoop file systems are automatically available when `flink-runtime` and the Hadoop libraries are in classpath.

This way, Flink seamlessly supports all of Hadoop file systems, and all Hadoop-compatible file systems (HCFS).

  - **hdfs**
  - **ftp**
  - **s3n** and **s3a**
  - **har**
  - ...

## Adding new File System Implementations

File systems are represented via the `org.apache.flink.core.fs.FileSystem` class, which captures the ways to access and modify files and objects in that file system. 
Implementations are discovered by Flink through Java's service abstraction, making it easy to add additional file system implementations.

In order to add a new file system, the following steps are needed:

  - Add the File System implementation, which is a subclass of `org.apache.flink.core.fs.FileSystem`.
  - Add a factory that instantiates that file system and declares the scheme under which the FileSystem is registered. This must be a subclass of `org.apache.flink.core.fs.FileSystemFactory`.
  - Add a service entry. Create a file `META-INF/services/org.apache.flink.core.fs.FileSystemFactory` which contains the class name of your file system factory class.

See the [Java Service Loader docs](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) for more details on how service loaders work.

{% top %}
