---
title: "File Systems"
weight: 11
type: docs
aliases:
  - /internals/filesystems.html
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

# File Systems

Flink has its own file system abstraction via the `org.apache.flink.core.fs.FileSystem` class.
This abstraction provides a common set of operations and minimal guarantees across various types
of file system implementations.

The `FileSystem`'s set of available operations is quite limited, in order to support a wide
range of file systems. For example, appending to or mutating existing files is not supported.

File systems are identified by a *file system scheme*, such as `file://`, `hdfs://`, etc.

# Implementations

Flink implements the file systems directly, with the following file system schemes:

  - `file`, which represents the machine's local file system.

Other file system types are accessed by an implementation that bridges to the suite of file systems supported by
[Apache Hadoop](https://hadoop.apache.org/). The following is an incomplete list of examples:

  - `hdfs`: Hadoop Distributed File System
  - `s3`, `s3n`, and `s3a`: Amazon S3 file system
  - `gcs`: Google Cloud Storage
  - `maprfs`: The MapR distributed file system
  - ...

Flink loads Hadoop's file systems transparently if it finds the Hadoop File System classes in the class path and finds a valid
Hadoop configuration. By default, it looks for the Hadoop configuration in the class path. Alternatively, one can specify a
custom location via the configuration entry `fs.hdfs.hadoopconf`.


# Persistence Guarantees

These `FileSystem` and its `FsDataOutputStream` instances are used to persistently store data, both for results of applications
and for fault tolerance and recovery. It is therefore crucial that the persistence semantics of these streams are well defined.

## Definition of Persistence Guarantees

Data written to an output stream is considered persistent, if two requirements are met:

  1. **Visibility Requirement:** It must be guaranteed that all other processes, machines,
     virtual machines, containers, etc. that are able to access the file see the data consistently
     when given the absolute file path. This requirement is similar to the *close-to-open*
     semantics defined by POSIX, but restricted to the file itself (by its absolute path).

  2. **Durability Requirement:** The file system's specific durability/persistence requirements
     must be met. These are specific to the particular file system. For example the
     {@link LocalFileSystem} does not provide any durability guarantees for crashes of both
     hardware and operating system, while replicated distributed file systems (like HDFS)
     guarantee typically durability in the presence of up *n* concurrent node failures,
     where *n* is the replication factor.

Updates to the file's parent directory (such that the file shows up when
listing the directory contents) are not required to be complete for the data in the file stream
to be considered persistent. This relaxation is important for file systems where updates to
directory contents are only eventually consistent.

The `FSDataOutputStream` has to guarantee data persistence for the written bytes once the call to
`FSDataOutputStream.close()` returns.

## Examples
 
  - For **fault-tolerant distributed file systems**, data is considered persistent once 
    it has been received and acknowledged by the file system, typically by having been replicated
    to a quorum of machines (*durability requirement*). In addition the absolute file path
    must be visible to all other machines that will potentially access the file (*visibility requirement*).

    Whether data has hit non-volatile storage on the storage nodes depends on the specific
    guarantees of the particular file system.

    The metadata updates to the file's parent directory are not required to have reached
    a consistent state. It is permissible that some machines see the file when listing the parent
    directory's contents while others do not, as long as access to the file by its absolute path
    is possible on all nodes.

  - A **local file system** must support the POSIX *close-to-open* semantics.
    Because the local file system does not have any fault tolerance guarantees, no further
    requirements exist.
 
    The above implies specifically that data may still be in the OS cache when considered
    persistent from the local file system's perspective. Crashes that cause the OS cache to lose
    data are considered fatal to the local machine and are not covered by the local file system's
    guarantees as defined by Flink.

    That means that computed results, checkpoints, and savepoints that are written only to
    the local filesystem are not guaranteed to be recoverable from the local machine's failure,
    making local file systems unsuitable for production setups.

# Updating File Contents

Many file systems either do not support overwriting contents of existing files at all, or do not support consistent visibility of the
updated contents in that case. For that reason, Flink's FileSystem does not support appending to existing files, or seeking within
output streams such that previously written data could be changed within the same file.

# Overwriting Files

Overwriting files is in general possible. A file is overwritten by deleting it and creating a new file.
However, certain filesystems cannot make that change synchronously visible to all parties that have access to the file.
For example [Amazon S3](https://aws.amazon.com/documentation/s3/) guarantees only *eventual consistency* in
the visibility of the file replacement: Some machines may see the old file, some machines may see the new file.

To avoid these consistency issues, the implementations of failure/recovery mechanisms in Flink strictly avoid writing to
the same file path more than once.

# Thread Safety

Implementations of `FileSystem` must be thread-safe: The same instance of `FileSystem` is frequently shared across multiple threads
in Flink and must be able to concurrently create input/output streams and list file metadata.

The `FSDataOutputStream` and `FSDataOutputStream` implementations are strictly **not thread-safe**.
Instances of the streams should also not be passed between threads in between read or write operations, because there are no guarantees
about the visibility of operations across threads (many operations do not create memory fences).

{{< top >}}
