---
title: "文件系统"
nav-id: filesystems
nav-parent_id: deployment
nav-show_overview: true
nav-pos: 5
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

Apache Flink 使用文件系统来消费和持久化地存储数据，以处理应用结果以及容错与恢复。以下是一些最常用的文件系统：*本地存储*，*hadoop-compatible*，*Amazon S3*，*MapR FS*，*OpenStack Swift FS*，*阿里云 OSS* 和 *Azure Blob Storage*。

文件使用的文件系统通过其 URI Scheme 指定。例如 `file:///home/user/text.txt` 表示一个在本地文件系统中的文件，`hdfs://namenode:50010/data/user/text.txt` 表示一个在指定 HDFS 集群中的文件。

文件系统在每个进程实例化一次，然后进行缓存/池化，从而避免每次创建流时的配置开销，并强制执行特定的约束，如连接/流的限制。

* This will be replaced by the TOC
{:toc}

## 本地文件系统

Flink 原生支持本地机器上的文件系统，包括任何挂载到本地文件系统的 NFS 或 SAN 驱动器，默认即可使用，无需额外配置。本地文件可通过 *file://* URI Scheme 引用。

## 外部文件系统

Apache Flink 支持下列文件系统：
  - [**Amazon S3**]({% link deployment/filesystems/s3.zh.md %}) 对象存储由 `flink-s3-fs-presto` 和 `flink-s3-fs-hadoop` 两种替代实现提供支持。这两种实现都是独立的，没有依赖项。

  - **MapR FS** 文件系统适配器已在 Flink 的主发行版中通过 *maprfs://* URI Scheme 支持。MapR 库需要在 classpath 中指定（例如在 `lib` 目录中）。

  - **OpenStack Swift FS** 由 `flink-swift-fs-hadoop` 支持，并通过 *swift://* URI scheme 使用。该实现基于 [Hadoop Project](https://hadoop.apache.org/)，但其是独立的，没有依赖项。
  将 Flink 作为库使用时，使用该文件系统需要添加相应的 Maven 依赖项（`org.apache.flink:flink-swift-fs-hadoop:{{ site.version }}`）。

  - **[阿里云对象存储]({% link deployment/filesystems/oss.zh.md %})**由 `flink-oss-fs-hadoop` 支持，并通过 *oss://* URI scheme 使用。该实现基于 [Hadoop Project](https://hadoop.apache.org/)，但其是独立的，没有依赖项。

  - **[Azure Blob Storage]({% link deployment/filesystems/azure.zh.md %})** 由`flink-azure-fs-hadoop` 支持，并通过 *wasb(s)://* URI scheme 使用。该实现基于 [Hadoop Project](https://hadoop.apache.org/)，但其是独立的，没有依赖项。

除 **MapR FS** 之外，上述文件系统可以并且需要作为[插件]({% link deployment/filesystems/plugins.zh.md %})使用。

使用外部文件系统时，在启动 Flink 之前需将对应的 JAR 文件从 `opt` 目录复制到 Flink 发行版 `plugin` 目录下的某一文件夹中，例如：

{% highlight bash %}
mkdir ./plugins/s3-fs-hadoop
cp ./opt/flink-s3-fs-hadoop-{{ site.version }}.jar ./plugins/s3-fs-hadoop/
{% endhighlight %}

<span class="label label-danger">注意</span> 文件系统的[插件]({% link deployment/filesystems/plugins.zh.md %})机制在 Flink 版本 1.9 中引入，以支持每个插件专有 Java 类加载器，并避免类隐藏机制。您仍然可以通过旧机制使用文件系统，即将对应的 JAR 文件复制到 `lib` 目录中，或使用您自己的实现方式，但是从版本 1.10 开始，**S3 插件必须通过插件机制加载**，因为这些插件不再被隐藏（版本 1.10 之后类不再被重定位），旧机制不再可用。

尽可能通过基于[插件]({% link deployment/filesystems/plugins.zh.md %})的加载机制使用支持的文件系统。未来的 Flink 版本将不再支持通过 `lib` 目录加载文件系统组件。

## 添加新的外部文件系统实现

文件系统由类 `org.apache.flink.core.fs.FileSystem` 表示，该类定义了访问与修改文件系统中文件与对象的方法。

要添加一个新的文件系统：

  - 添加文件系统实现，它应是 `org.apache.flink.core.fs.FileSystem` 的子类。
  - 添加 Factory 类，以实例化该文件系统并声明文件系统所注册的 scheme, 它应是 `org.apache.flink.core.fs.FileSystemFactory` 的子类。
  - 添加 Service Entry。创建文件 `META-INF/services/org.apache.flink.core.fs.FileSystemFactory`，文件中包含文件系统 Factory 类的类名。
  （更多细节请查看 [Java Service Loader docs](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html)）

在插件检索时，文件系统 Factory 类会由一个专用的 Java 类加载器加载，从而避免与其他类或 Flink 组件冲突。在文件系统实例化和文件系统调用时，应使用该类加载器。

<span class="label label-warning">警告</span> 实际上这表示您的实现应避免使用 `Thread.currentThread().getContextClassLoader()` 类加载器。

## Hadoop 文件系统 (HDFS) 及其其他实现

所有 Flink 无法找到直接支持的文件系统均将回退为 Hadoop。
当 flink-runtime 和 Hadoop 类包含在 classpath 中时，所有的 Hadoop 文件系统将自动可用。


因此，Flink 无缝支持所有实现 `org.apache.hadoop.fs.FileSystem` 接口的 Hadoop 文件系统和所有兼容 Hadoop 的文件系统 (Hadoop-compatible file system, HCFS)：
  - HDFS （已测试）
  - [Google Cloud Storage Connector for Hadoop](https://cloud.google.com/hadoop/google-cloud-storage-connector)（已测试）
  - [Alluxio](http://alluxio.org/)（已测试，参见下文的配置详细信息）
  - [XtreemFS](http://www.xtreemfs.org/)（已测试）
  - FTP via [Hftp](http://hadoop.apache.org/docs/r1.2.1/hftp.html)（未测试）
  - HAR（未测试）
  - ...

Hadoop 配置须在 `core-site.xml` 文件中包含所需文件系统的实现。可查看 **[Alluxio 的示例](#alluxio)**。

除非有其他的需要，建议使用 Flink 内置的文件系统。在某些情况下，如通过配置 Hadoop `core-site.xml` 中的 `fs.defaultFS` 属性将文件系统作为 YARN 的资源存储时，可能需要直接使用 Hadoop 文件系统。

### Alluxio

在 `core-site.xml` 文件中添加以下条目以支持 Alluxio：

{% highlight xml %}
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
{% endhighlight %}

{% top %}
