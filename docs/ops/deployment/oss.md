---
title: "Aliyun Object Storage Service (OSS)"
nav-title: Aliyun OSS
nav-parent_id: deployment
nav-pos: 9
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


## OSS: Object Storage Service

[Aliyun Object Storage Service](https://www.aliyun.com/product/oss) (Aliyun OSS) is widely used especially among China’s cloud users, and it provides cloud object storage for a variety of use cases.

[Hadoop file system](http://hadoop.apache.org/docs/current/hadoop-aliyun/tools/hadoop-aliyun/index.html) supports OSS since version 2.9.1. Now, you can also use OSS with Fink for **reading** and **writing data**.

You can access OSS objects like this:

{% highlight plain %}
oss://<your-bucket>/<object-name>
{% endhighlight %}

Below shows how to use OSS with Flink:

{% highlight java %}
// Read from OSS bucket
env.readTextFile("oss://<your-bucket>/<object-name>");

// Write to OSS bucket
dataSet.writeAsText("oss://<your-bucket>/<object-name>")

{% endhighlight %}

There are two ways to use OSS with Flink, our shaded `flink-oss-fs-hadoop` will cover most scenarios. However, you may need to set up a specific Hadoop OSS FileSystem implementation if you want use OSS as YARN's resource storage dir ([This patch](https://issues.apache.org/jira/browse/HADOOP-15919) enables YARN to use OSS). Both ways are described below.

### Shaded Hadoop OSS file system (recommended)

In order to use `flink-oss-fs-hadoop`, copy the respective JAR file from the opt directory to the lib directory of your Flink distribution before starting Flink, e.g.

{% highlight bash %}
cp ./opt/flink-oss-fs-hadoop-{{ site.version }}.jar ./lib/
{% endhighlight %}

`flink-oss-fs-hadoop` registers default FileSystem wrappers for URIs with the oss:// scheme.

#### Configurations setup
After setting up the OSS FileSystem wrapper, you need to add some configurations to make sure that Flink is allowed to access your OSS buckets.

In order to use OSS with Flink more easily, you can use the same configuration keys in `flink-conf.yaml` as in Hadoop's `core-site.xml`

You can see the configuration keys in the [Hadoop OSS documentation](http://hadoop.apache.org/docs/current/hadoop-aliyun/tools/hadoop-aliyun/index.html).

There are some required configurations that must be added to `flink-conf.yaml` (**Other configurations defined in Hadoop OSS documentation are advanced configurations which used by performance tuning**):

{% highlight yaml %}
fs.oss.endpoint: Aliyun OSS endpoint to connect to
fs.oss.accessKeyId: Aliyun access key ID
fs.oss.accessKeySecret: Aliyun access key secret
{% endhighlight %}

### Hadoop-provided OSS file system - manual setup
This setup is a bit more complex and we recommend using our shaded Hadoop file systems instead (see above) unless required otherwise, e.g. for using OSS as YARN’s resource storage dir via the fs.defaultFS configuration property in Hadoop’s core-site.xml.

#### Set OSS FileSystem
You need to point Flink to a valid Hadoop configuration, which contains the following properties in core-site.xml:

{% highlight xml %}
<configuration>

<property>
    <name>fs.oss.impl</name>
    <value>org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem</value>
  </property>

  <property>
    <name>fs.oss.endpoint</name>
    <value>Aliyun OSS endpoint to connect to</value>
    <description>Aliyun OSS endpoint to connect to. An up-to-date list is provided in the Aliyun OSS Documentation.</description>
  </property>

  <property>
    <name>fs.oss.accessKeyId</name>
    <description>Aliyun access key ID</description>
  </property>

  <property>
    <name>fs.oss.accessKeySecret</name>
    <description>Aliyun access key secret</description>
  </property>

  <property>
    <name>fs.oss.buffer.dir</name>
    <value>/tmp/oss</value>
  </property>

</property>

</configuration>
{% endhighlight %}

#### Hadoop Configuration

You can specify the [Hadoop configuration](../config.html#hdfs) in various ways pointing Flink to
the path of the Hadoop configuration directory, for example
- by setting the environment variable `HADOOP_CONF_DIR`, or
- by setting the `fs.hdfs.hadoopconf` configuration option in `flink-conf.yaml`:
{% highlight yaml %}
fs.hdfs.hadoopconf: /path/to/etc/hadoop
{% endhighlight %}

This registers `/path/to/etc/hadoop` as Hadoop's configuration directory with Flink. Flink will look for the `core-site.xml` and `hdfs-site.xml` files in the specified directory.

#### Provide OSS FileSystem Dependency

You can find Hadoop OSS FileSystem are packaged in the hadoop-aliyun artifact. This JAR and all its dependencies need to be added to Flink’s classpath, i.e. the class path of both Job and TaskManagers.

There are multiple ways of adding JARs to Flink’s class path, the easiest being simply to drop the JARs in Flink’s lib folder. You need to copy the hadoop-aliyun JAR with all its dependencies (You can find these as part of the Hadoop binaries in hadoop-3/share/hadoop/tools/lib). You can also export the directory containing these JARs as part of the HADOOP_CLASSPATH environment variable on all machines.

## An Example
Below is an example shows the result of our setup (data is generated by TPC-DS tool)

{% highlight java %}
// Read from OSS bucket
scala> val dataSet = benv.readTextFile("oss://<your-bucket>/50/call_center/data-m-00049")
dataSet: org.apache.flink.api.scala.DataSet[String] = org.apache.flink.api.scala.DataSet@31940704

scala> dataSet.print()
1|AAAAAAAABAAAAAAA|1998-01-01|||2450952|NY Metro|large|2935|1670015|8AM-4PM|Bob Belcher|6|More than other authori|Shared others could not count fully dollars. New members ca|Julius Tran|3|pri|6|cally|730|Ash Hill|Boulevard|Suite 0|Oak Grove|Williamson County|TN|38370|United States|-5|0.11|
2|AAAAAAAACAAAAAAA|1998-01-01|2000-12-31||2450806|Mid Atlantic|medium|1574|594972|8AM-8AM|Felipe Perkins|2|A bit narrow forms matter animals. Consist|Largely blank years put substantially deaf, new others. Question|Julius Durham|5|anti|1|ought|984|Center Hill|Way|Suite 70|Midway|Williamson County|TN|31904|United States|-5|0.12|
3|AAAAAAAACAAAAAAA|2001-01-01|||2450806|Mid Atlantic|medium|1574|1084486|8AM-4PM|Mark Hightower|2|Wrong troops shall work sometimes in a opti|Largely blank years put substantially deaf, new others. Question|Julius Durham|1|ought|2|able|984|Center Hill|Way|Suite 70|Midway|Williamson County|TN|31904|United States|-5|0.01|
4|AAAAAAAAEAAAAAAA|1998-01-01|2000-01-01||2451063|North Midwest|medium|10137|6578913|8AM-4PM|Larry Mccray|2|Dealers make most historical, direct students|Rich groups catch longer other fears; future,|Matthew Clifton|4|ese|3|pri|463|Pine Ridge|RD|Suite U|Five Points|Ziebach County|SD|56098|United States|-6|0.05|
5|AAAAAAAAEAAAAAAA|2000-01-02|2001-12-31||2451063|North Midwest|small|17398|4610470|8AM-8AM|Larry Mccray|2|Dealers make most historical, direct students|Blue, due beds come. Politicians would not make far thoughts. Specifically new horses partic|Gary Colburn|4|ese|3|pri|463|Pine Ridge|RD|Suite U|Five Points|Ziebach County|SD|56098|United States|-6|0.12|
6|AAAAAAAAEAAAAAAA|2002-01-01|||2451063|North Midwest|medium|13118|6585236|8AM-4PM|Larry Mccray|5|Silly particles could pro|Blue, due beds come. Politicians would not make far thoughts. Specifically new horses partic|Gary Colburn|5|anti|3|pri|463|Pine Ridge|RD|Suite U|Five Points|Ziebach County|SD|56098|United States|-6|0.11|
7|AAAAAAAAHAAAAAAA|1998-01-01|||2451024|Pacific Northwest|small|6280|1739560|8AM-4PM|Alden Snyder|6|Major, formal states can suppor|Reduced, subsequent bases could not lik|Frederick Weaver|5|anti|4|ese|415|Jefferson Tenth|Court|Suite 180|Riverside|Walker County|AL|39231|United States|-6|0.00|
8|AAAAAAAAIAAAAAAA|1998-01-01|2000-12-31||2450808|California|small|4766|2459256|8AM-12AM|Wayne Ray|6|Here possible notions arrive only. Ar|Common, free creditors should exper|Daniel Weller|5|anti|2|able|550|Cedar Elm|Ct.|Suite I|Fairview|Williamson County|TN|35709|United States|-5|0.06|

scala> dataSet.count()
res0: Long = 8

// Write to OSS bucket
scala> dataSet.writeAsText("oss://<your-bucket>/50/call_center/data-m-00049.1")

scala> benv.execute("My batch program")
res1: org.apache.flink.api.common.JobExecutionResult = org.apache.flink.api.common.JobExecutionResult@77476fcf

scala> val newDataSet = benv.readTextFile("oss://<your-bucket>/50/call_center/data-m-00049.1")
newDataSet: org.apache.flink.api.scala.DataSet[String] = org.apache.flink.api.scala.DataSet@40b70f31

scala> newDataSet.count()
res2: Long = 8

{% endhighlight %}

## Common Issues
### Could not find OSS file system
If your job submission fails with an Exception message like below, please check if our shaded jar (flink-oss-fs-hadoop-{{ site.version }}.jar) is in the lib dir.

{% highlight plain %}
Caused by: org.apache.flink.runtime.client.JobExecutionException: Could not set up JobManager
	at org.apache.flink.runtime.jobmaster.JobManagerRunner.<init>(JobManagerRunner.java:176)
	at org.apache.flink.runtime.dispatcher.Dispatcher$DefaultJobManagerRunnerFactory.createJobManagerRunner(Dispatcher.java:1058)
	at org.apache.flink.runtime.dispatcher.Dispatcher.lambda$createJobManagerRunner$5(Dispatcher.java:308)
	at org.apache.flink.util.function.CheckedSupplier.lambda$unchecked$0(CheckedSupplier.java:34)
	... 7 more
Caused by: org.apache.flink.runtime.JobException: Creating the input splits caused an error: Could not find a file system implementation for scheme 'oss'. The scheme is not directly supported by Flink and no Hadoop file system to support this scheme could be loaded.
	at org.apache.flink.runtime.executiongraph.ExecutionJobVertex.<init>(ExecutionJobVertex.java:273)
	at org.apache.flink.runtime.executiongraph.ExecutionGraph.attachJobGraph(ExecutionGraph.java:827)
	at org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder.buildGraph(ExecutionGraphBuilder.java:232)
	at org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder.buildGraph(ExecutionGraphBuilder.java:100)
	at org.apache.flink.runtime.jobmaster.JobMaster.createExecutionGraph(JobMaster.java:1151)
	at org.apache.flink.runtime.jobmaster.JobMaster.createAndRestoreExecutionGraph(JobMaster.java:1131)
	at org.apache.flink.runtime.jobmaster.JobMaster.<init>(JobMaster.java:294)
	at org.apache.flink.runtime.jobmaster.JobManagerRunner.<init>(JobManagerRunner.java:157)
	... 10 more
Caused by: org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Could not find a file system implementation for scheme 'oss'. The scheme is not directly supported by Flink and no Hadoop file system to support this scheme could be loaded.
	at org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(FileSystem.java:403)
	at org.apache.flink.core.fs.FileSystem.get(FileSystem.java:318)
	at org.apache.flink.core.fs.Path.getFileSystem(Path.java:298)
	at org.apache.flink.api.common.io.FileInputFormat.createInputSplits(FileInputFormat.java:587)
	at org.apache.flink.api.common.io.FileInputFormat.createInputSplits(FileInputFormat.java:62)
	at org.apache.flink.runtime.executiongraph.ExecutionJobVertex.<init>(ExecutionJobVertex.java:259)
	... 17 more
Caused by: org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Hadoop is not in the classpath/dependencies.
	at org.apache.flink.core.fs.UnsupportedSchemeFactory.create(UnsupportedSchemeFactory.java:64)
	at org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(FileSystem.java:399)
	... 22 more
{% endhighlight %}

### Missing configuration(s)
If your job submission fails with an Exception message like below, please check if the corresponding configurations exits in `flink-conf.yaml`

{% highlight plain %}
Caused by: org.apache.flink.runtime.JobException: Creating the input splits caused an error: Aliyun OSS endpoint should not be null or empty. Please set proper endpoint with 'fs.oss.endpoint'.
	at org.apache.flink.runtime.executiongraph.ExecutionJobVertex.<init>(ExecutionJobVertex.java:273)
	at org.apache.flink.runtime.executiongraph.ExecutionGraph.attachJobGraph(ExecutionGraph.java:827)
	at org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder.buildGraph(ExecutionGraphBuilder.java:232)
	at org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder.buildGraph(ExecutionGraphBuilder.java:100)
	at org.apache.flink.runtime.jobmaster.JobMaster.createExecutionGraph(JobMaster.java:1151)
	at org.apache.flink.runtime.jobmaster.JobMaster.createAndRestoreExecutionGraph(JobMaster.java:1131)
	at org.apache.flink.runtime.jobmaster.JobMaster.<init>(JobMaster.java:294)
	at org.apache.flink.runtime.jobmaster.JobManagerRunner.<init>(JobManagerRunner.java:157)
	... 10 more
Caused by: java.lang.IllegalArgumentException: Aliyun OSS endpoint should not be null or empty. Please set proper endpoint with 'fs.oss.endpoint'.
	at org.apache.flink.fs.shaded.hadoop3.org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore.initialize(AliyunOSSFileSystemStore.java:145)
	at org.apache.flink.fs.shaded.hadoop3.org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem.initialize(AliyunOSSFileSystem.java:323)
	at org.apache.flink.fs.osshadoop.OSSFileSystemFactory.create(OSSFileSystemFactory.java:87)
	at org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(FileSystem.java:395)
	at org.apache.flink.core.fs.FileSystem.get(FileSystem.java:318)
	at org.apache.flink.core.fs.Path.getFileSystem(Path.java:298)
	at org.apache.flink.api.common.io.FileInputFormat.createInputSplits(FileInputFormat.java:587)
	at org.apache.flink.api.common.io.FileInputFormat.createInputSplits(FileInputFormat.java:62)
	at org.apache.flink.runtime.executiongraph.ExecutionJobVertex.<init>(ExecutionJobVertex.java:259)
	... 17 more
{% endhighlight %}
