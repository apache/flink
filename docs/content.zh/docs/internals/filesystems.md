---
title: "文件系统"
weight: 11
type: docs
aliases:
  - /zh/internals/filesystems.html
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

# 文件系统

Flink 通过 `org.apache.flink.core.fs.FileSystem` 这个类实现了自有的文件系统。抽象出了一组通用操作以及实现不同种类文件系统的最小保证。

`FileSystem` 所提供的操作十分有限，是因为为了支持更多的文件系统。例如, 不支持追加或更改现有文件。

通过 *file system scheme* 对不同文件系统进行定义，例如 `file://` 、`hdfs://` 等。

<a name="implementations"></a>

# 实现

Flink 通过如下格式直接实现文件系统：

  - `file`，代表本地文件系统。

Flink 还支持通过 [Apache Hadoop](https://hadoop.apache.org/) 与其他的文件系统进行连接。如下所示：

  - `hdfs`：代表 Hadoop 分布式文件系统
  - `s3`、`s3n` 和 `s3a`：代表 Amazon S3 文件系统
  - `gcs`：代表 Google 云存储系统
  - ...

如果 Flink 在类路径中找到了 Hadoop 文件系统类，并且找到有效的 Hadoop 配置文件。默认情况下，它会在类路径中查找 Hadoop 配置文件。或者，可以通过配置属性 `fs.hdfs.hadoopconf` 来自定义 Hadoop 配置文件的路径。

<a name="persistence-guarantees"></a>

# 持久化保证

文件系统利用 `FileSystem` 和 `FsDataOutputStream` 的实例对应用程序的结果数据以及容错和恢复的数据进行持久化，因此很好地定义这些流的持久性语义至关重要。

<a name="definition-of-persistence-guarantees"></a>

## 持久化保证定义

如果满足以下两个要求，写入输出流的数据就会被认为是持久的：

  1、 **可见性要求：** 所有的流程、机器、虚拟机、容器等都要保证这种要求。当给定绝对文件路径时，能够访问该文件的用户可以一致地查看数据 。此要求类似于由 POSIX 定义的 *close-to-open* 语义，但仅限于文件本身（通过其绝对路径）。

  2、 **耐久性要求：** 文件系统的特定耐久性/持久性要求必须满足。这些是特定于特定文件系统的。例如，在复制式分布式文件系统（如 HDFS）出现多达 *n* 个并发节点故障并且需要保证耐久性时，{@link LocalFileSystem} 在硬件和操作系统这两种情况下出现崩溃时是不提供任何持久性保证的，其中 *n* 代表复制因子。

不需要完成对文件父目录的更新（当列出目录内容时，文件就会显示出来），就可以将文件流中的数据视为持久数据。这种放松对于目录内容更新最终保持一致的文件系统来说很重要。

当 `FSDataOutputStream.close()` 调用完成时， `FSDataOutputStream` 必须保证写入的字节数据的持久性。

<a name="examples"></a>

## 例子
 
  - 对于 **容错分布式文件系统**, 数据一旦被文件系统接收并确认，就被认为是持久的，通常是复制到一组机器上（*耐久性要求*）。此外，绝对文件路径必须对可能访问该文件对所有其他机器可见（*可见性要求*）。

    数据是否到达存储节点上的非易失性存储取决于特定文件系统的具体保证。

    对文件父目录的元数据更新不需要达到一致状态。只要所有节点都可以通过其绝对路径访问该文件，那么是允许某些机器在列出父目录的内容时看到该文件，而其他机器则看不到。

  - **本地文件系统** 必须支持由 POSIX 定义的 *close-to-open* 语义。
    由于本地文件系统没有任何容错保证，因此不存在进一步的要求。
 
    以上特别说明，从本地文件系统的角度来看，当数据被认为是持久的时，数据可能仍在操作系统缓存中。导致操作系统缓存丢失数据的崩溃被认为对本地计算机是致命的，并且不在 Flink 定义的本地文件系统保证范围内。

    这意味着，仅写入本地文件系统的计算结果、检查点和保存点不能保证从本地机器故障中恢复，使得本地文件系统不适合用于生产环境。

<a name="updating-file-contents"></a>

# 更新文件内容

许多文件系统要么根本不支持覆盖现有文件的内容，要么在某种情况下不支持更新内容的一致性和可见性。出于这个原因，为了在同一文件中修改以前写入的数据，Flink 文件系统不支持追加数据到现有文件或者在输出流中进行查找。

<a name="overwriting-files"></a>

# 覆盖文件

覆盖文件通常是可行的。通过删除文件然后创建新文件来覆盖文件。例如，[Amazon S3](https://aws.amazon.com/documentation/s3/) 在文件替换的可见性方面只保证最终一致性：有些机器可能会看到旧文件，有些机器可能会看到新文件。

为了避免一致性问题，Flink 通过实现故障/恢复机制来严格避免多次写入相同路径文件。

<a name="thread-safety"></a>

# 线程安全

`FileSystem` 的实现必须是线程安全的：Flink 中的多个线程经常共享 `FileSystem` 的同一个实例，并且必须能够同时创建输入/输出流和列出文件元数据。

严格来说，`FSDataOutputStream` 和 `FSDataOutputStream` 的实现是 **非线程安全的**。因为它们不能保证关于跨线程操作的可见性，那么它们就不应该在读或写操作线程之间进行传递（许多操作不会创建内存围栏）。

{{< top >}}
