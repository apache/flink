---
title: "Amazon Web Services (AWS)"
nav-title: AWS
nav-parent_id: deployment
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

Amazon Web Services offers cloud computing services on which you can run Flink.

* ToC
{:toc}

## EMR: Elastic MapReduce

[Amazon Elastic MapReduce](https://aws.amazon.com/elasticmapreduce/) (Amazon EMR) is a web service that makes it easy to  quickly setup a Hadoop cluster. This is the **recommended way** to run Flink on AWS as it takes care of setting up everything.

### Standard EMR Installation

Flink is a supported application on Amazon EMR. [Amazon's documentation](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-flink.html)
describes configuring Flink, creating and monitoring a cluster, and working with jobs.

### Custom EMR Installation

Amazon EMR services are regularly updated to new releases but a version of Flink which is not available
can be manually installed in a stock EMR cluster.

**Create EMR Cluster**

The EMR documentation contains [examples showing how to start an EMR cluster](http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-gs-launch-sample-cluster.html). You can follow that guide and install any EMR release. You don't need to install the *All Applications* part of the EMR release, but can stick to *Core Hadoop*.

{% warn Note %}
Access to S3 buckets requires
[configuration of IAM roles](http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-iam-roles.html)
when creating an EMR cluster.

**Install Flink on EMR Cluster**

After creating your cluster, you can [connect to the master node](http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-connect-master-node.html) and install Flink:

1. Go the [Downloads Page]({{ site.download_url }}) and **download a binary version of Flink matching the Hadoop version** of your EMR cluster, e.g. Hadoop 2.7 for EMR releases 4.3.0, 4.4.0, or 4.5.0.
2. Extract the Flink distribution and you are ready to deploy [Flink jobs via YARN](yarn_setup.html) after **setting the Hadoop config directory**:

{% highlight bash %}
HADOOP_CONF_DIR=/etc/hadoop/conf ./bin/flink run -m yarn-cluster -yn 1 examples/streaming/WordCount.jar
{% endhighlight %}

{% top %}

## S3: Simple Storage Service

[Amazon Simple Storage Service](http://aws.amazon.com/s3/) (Amazon S3) provides cloud object storage for a variety of use cases. You can use S3 with Flink for **reading** and **writing data** as well in conjunction with the [streaming **state backends**]({{ site.baseurl}}/ops/state/state_backends.html) or even as a YARN object storage.

You can use S3 objects like regular files by specifying paths in the following format:

{% highlight plain %}
s3://<your-bucket>/<endpoint>
{% endhighlight %}

The endpoint can either be a single file or a directory, for example:

{% highlight java %}
// Read from S3 bucket
env.readTextFile("s3://<bucket>/<endpoint>");

// Write to S3 bucket
stream.writeAsText("s3://<bucket>/<endpoint>");

// Use S3 as FsStatebackend
env.setStateBackend(new FsStateBackend("s3://<your-bucket>/<endpoint>"));
{% endhighlight %}

Note that these examples are *not* exhaustive and you can use S3 in other places as well, including your [high availability setup](../jobmanager_high_availability.html) or the [RocksDBStateBackend]({{ site.baseurl }}/ops/state/state_backends.html#the-rocksdbstatebackend); everywhere that Flink expects a FileSystem URI.

For most use cases, you may use one of our shaded `flink-s3-fs-hadoop` and `flink-s3-fs-presto` S3
filesystem wrappers which are fairly easy to set up. For some cases, however, e.g. for using S3 as
YARN's resource storage dir, it may be necessary to set up a specific Hadoop S3 FileSystem
implementation. Both ways are described below.

### Shaded Hadoop/Presto S3 file systems (recommended)

{% panel **Note:** You don't have to configure this manually if you are running [Flink on EMR](#emr-elastic-mapreduce). %}

To use either `flink-s3-fs-hadoop` or `flink-s3-fs-presto`, copy the respective JAR file from the
`opt` directory to the `lib` directory of your Flink distribution before starting Flink, e.g.

{% highlight bash %}
cp ./opt/flink-s3-fs-presto-{{ site.version }}.jar ./lib/
{% endhighlight %}

#### Configure Access Credentials

After setting up the S3 FileSystem wrapper, you need to make sure that Flink is allowed to access your S3 buckets.

##### Identity and Access Management (IAM) (Recommended)

The recommended way of setting up credentials on AWS is via [Identity and Access Management (IAM)](http://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html). You can use IAM features to securely give Flink instances the credentials that they need in order to access S3 buckets. Details about how to do this are beyond the scope of this documentation. Please refer to the AWS user guide. What you are looking for are [IAM Roles](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html).

If you set this up correctly, you can manage access to S3 within AWS and don't need to distribute any access keys to Flink.

##### Access Keys (Discouraged)

Access to S3 can be granted via your **access and secret key pair**. Please note that this is discouraged since the [introduction of IAM roles](https://blogs.aws.amazon.com/security/post/Tx1XG3FX6VMU6O5/A-safer-way-to-distribute-AWS-credentials-to-EC2).

You need to configure both `s3.access-key` and `s3.secret-key`  in Flink's  `flink-conf.yaml`:

{% highlight yaml %}
s3.access-key: your-access-key
s3.secret-key: your-secret-key
{% endhighlight %}

{% top %}

### Hadoop-provided S3 file systems - manual setup

{% panel **Note:** You don't have to configure this manually if you are running [Flink on EMR](#emr-elastic-mapreduce). %}

This setup is a bit more complex and we recommend using our shaded Hadoop/Presto file systems
instead (see above) unless required otherwise, e.g. for using S3 as YARN's resource storage dir
via the `fs.defaultFS` configuration property in Hadoop's `core-site.xml`.

#### Set S3 FileSystem

Interaction with S3 happens via one of [Hadoop's S3 FileSystem clients](https://wiki.apache.org/hadoop/AmazonS3):

1. `S3AFileSystem` (**recommended** for Hadoop 2.7 and later): file system for reading and writing regular files using Amazon's SDK internally. No maximum file size and works with IAM roles.
2. `NativeS3FileSystem` (for Hadoop 2.6 and earlier): file system for reading and writing regular files. Maximum object size is 5GB and does not work with IAM roles.

##### `S3AFileSystem` (Recommended)

This is the recommended S3 FileSystem implementation to use. It uses Amazon's SDK internally and works with IAM roles (see [Configure Access Credentials](#configure-access-credentials-1)).

You need to point Flink to a valid Hadoop configuration, which contains the following properties in `core-site.xml`:

{% highlight xml %}
<configuration>

<property>
  <name>fs.s3.impl</name>
  <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
</property>

<!-- Comma separated list of local directories used to buffer
     large results prior to transmitting them to S3. -->
<property>
  <name>fs.s3a.buffer.dir</name>
  <value>/tmp</value>
</property>

</configuration>
{% endhighlight %}

This registers `S3AFileSystem` as the default FileSystem for URIs with the `s3a://` scheme.

##### `NativeS3FileSystem`

This file system is limited to files up to 5GB in size and it does not work with IAM roles (see [Configure Access Credentials](#configure-access-credentials-1)), meaning that you have to manually configure your AWS credentials in the Hadoop config file.

You need to point Flink to a valid Hadoop configuration, which contains the following property in `core-site.xml`:

{% highlight xml %}
<property>
  <name>fs.s3.impl</name>
  <value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value>
</property>
{% endhighlight %}

This registers `NativeS3FileSystem` as the default FileSystem for URIs with the `s3://` scheme.

{% top %}

#### Hadoop Configuration

You can specify the [Hadoop configuration](../config.html#hdfs) in various ways pointing Flink to
the path of the Hadoop configuration directory, for example
- by setting the environment variable `HADOOP_CONF_DIR`, or
- by setting the `fs.hdfs.hadoopconf` configuration option in `flink-conf.yaml`:
{% highlight yaml %}
fs.hdfs.hadoopconf: /path/to/etc/hadoop
{% endhighlight %}

This registers `/path/to/etc/hadoop` as Hadoop's configuration directory with Flink. Flink will look for the `core-site.xml` and `hdfs-site.xml` files in the specified directory.

{% top %}

#### Configure Access Credentials

{% panel **Note:** You don't have to configure this manually if you are running [Flink on EMR](#emr-elastic-mapreduce). %}

After setting up the S3 FileSystem, you need to make sure that Flink is allowed to access your S3 buckets.

##### Identity and Access Management (IAM) (Recommended)

When using `S3AFileSystem`, the recommended way of setting up credentials on AWS is via [Identity and Access Management (IAM)](http://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html). You can use IAM features to securely give Flink instances the credentials that they need in order to access S3 buckets. Details about how to do this are beyond the scope of this documentation. Please refer to the AWS user guide. What you are looking for are [IAM Roles](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html).

If you set this up correctly, you can manage access to S3 within AWS and don't need to distribute any access keys to Flink.

Note that this only works with `S3AFileSystem` and not `NativeS3FileSystem`.

{% top %}

##### Access Keys with `S3AFileSystem` (Discouraged)

Access to S3 can be granted via your **access and secret key pair**. Please note that this is discouraged since the [introduction of IAM roles](https://blogs.aws.amazon.com/security/post/Tx1XG3FX6VMU6O5/A-safer-way-to-distribute-AWS-credentials-to-EC2).

For `S3AFileSystem` you need to configure both `fs.s3a.access.key` and `fs.s3a.secret.key`  in Hadoop's  `core-site.xml`:

{% highlight xml %}
<property>
  <name>fs.s3a.access.key</name>
  <value></value>
</property>

<property>
  <name>fs.s3a.secret.key</name>
  <value></value>
</property>
{% endhighlight %}

{% top %}

##### Access Keys with `NativeS3FileSystem` (Discouraged)

Access to S3 can be granted via your **access and secret key pair**. But this is discouraged and you should use `S3AFileSystem` [with the required IAM roles](https://blogs.aws.amazon.com/security/post/Tx1XG3FX6VMU6O5/A-safer-way-to-distribute-AWS-credentials-to-EC2).

For `NativeS3FileSystem` you need to configure both `fs.s3.awsAccessKeyId` and `fs.s3.awsSecretAccessKey`  in Hadoop's  `core-site.xml`:

{% highlight xml %}
<property>
  <name>fs.s3.awsAccessKeyId</name>
  <value></value>
</property>

<property>
  <name>fs.s3.awsSecretAccessKey</name>
  <value></value>
</property>
{% endhighlight %}

{% top %}

#### Provide S3 FileSystem Dependency

{% panel **Note:** You don't have to configure this manually if you are running [Flink on EMR](#emr-elastic-mapreduce). %}

Hadoop's S3 FileSystem clients are packaged in the `hadoop-aws` artifact (Hadoop version 2.6 and later). This JAR and all its dependencies need to be added to Flink's classpath, i.e. the class path of both Job and TaskManagers. Depending on which FileSystem implementation and which Flink and Hadoop version you use, you need to provide different dependencies (see below).

There are multiple ways of adding JARs to Flink's class path, the easiest being simply to drop the JARs in Flink's `lib` folder. You need to copy the `hadoop-aws` JAR with all its dependencies. You can also export the directory containing these JARs as part of the `HADOOP_CLASSPATH` environment variable on all machines.

##### Flink for Hadoop 2.7

Depending on which file system you use, please add the following dependencies. You can find these as part of the Hadoop binaries in `hadoop-2.7/share/hadoop/tools/lib`:

- `S3AFileSystem`:
  - `hadoop-aws-2.7.3.jar`
  - `aws-java-sdk-s3-1.11.183.jar` and its dependencies:
    - `aws-java-sdk-core-1.11.183.jar`
    - `aws-java-sdk-kms-1.11.183.jar`
    - `jackson-annotations-2.6.7.jar`
    - `jackson-core-2.6.7.jar`
    - `jackson-databind-2.6.7.jar`
    - `joda-time-2.8.1.jar`
    - `httpcore-4.4.4.jar`
    - `httpclient-4.5.3.jar`

- `NativeS3FileSystem`:
  - `hadoop-aws-2.7.3.jar`
  - `guava-11.0.2.jar`

Note that `hadoop-common` is available as part of Flink, but Guava is shaded by Flink.

##### Flink for Hadoop 2.6

Depending on which file system you use, please add the following dependencies. You can find these as part of the Hadoop binaries in `hadoop-2.6/share/hadoop/tools/lib`:

- `S3AFileSystem`:
  - `hadoop-aws-2.6.4.jar`
  - `aws-java-sdk-1.7.4.jar` and its dependencies:
    - `jackson-annotations-2.1.1.jar`
    - `jackson-core-2.1.1.jar`
    - `jackson-databind-2.1.1.jar`
    - `joda-time-2.2.jar`
    - `httpcore-4.2.5.jar`
    - `httpclient-4.2.5.jar`

- `NativeS3FileSystem`:
  - `hadoop-aws-2.6.4.jar`
  - `guava-11.0.2.jar`

Note that `hadoop-common` is available as part of Flink, but Guava is shaded by Flink.

##### Flink for Hadoop 2.4 and earlier

These Hadoop versions only have support for `NativeS3FileSystem`. This comes pre-packaged with Flink for Hadoop 2 as part of `hadoop-common`. You don't need to add anything to the classpath.

{% top %}

## Common Issues

The following sections lists common issues when working with Flink on AWS.

### Missing S3 FileSystem Configuration

If your job submission fails with an Exception message noting that `No file system found with scheme s3` this means that no FileSystem has been configured for S3. Please check out the configuration sections for our [shaded Hadoop/Presto](#shaded-hadooppresto-s3-file-systems-recommended) or [generic Hadoop](#set-s3-filesystem) file systems for details on how to configure this properly.

{% highlight plain %}
org.apache.flink.client.program.ProgramInvocationException: The program execution failed:
  Failed to submit job cd927567a81b62d7da4c18eaa91c3c39 (WordCount Example) [...]
Caused by: org.apache.flink.runtime.JobException: Creating the input splits caused an error:
  No file system found with scheme s3, referenced in file URI 's3://<bucket>/<endpoint>'. [...]
Caused by: java.io.IOException: No file system found with scheme s3,
  referenced in file URI 's3://<bucket>/<endpoint>'.
    at o.a.f.core.fs.FileSystem.get(FileSystem.java:296)
    at o.a.f.core.fs.Path.getFileSystem(Path.java:311)
    at o.a.f.api.common.io.FileInputFormat.createInputSplits(FileInputFormat.java:450)
    at o.a.f.api.common.io.FileInputFormat.createInputSplits(FileInputFormat.java:57)
    at o.a.f.runtime.executiongraph.ExecutionJobVertex.<init>(ExecutionJobVertex.java:156)
{% endhighlight %}

{% top %}

### AWS Access Key ID and Secret Access Key Not Specified

If you see your job failing with an Exception noting that the `AWS Access Key ID and Secret Access Key must be specified as the username or password`, your access credentials have not been set up properly. Please refer to the access credential section for our [shaded Hadoop/Presto](#configure-access-credentials) or [generic Hadoop](#configure-access-credentials-1) file systems for details on how to configure this.

{% highlight plain %}
org.apache.flink.client.program.ProgramInvocationException: The program execution failed:
  Failed to submit job cd927567a81b62d7da4c18eaa91c3c39 (WordCount Example) [...]
Caused by: java.io.IOException: The given file URI (s3://<bucket>/<endpoint>) points to the
  HDFS NameNode at <bucket>, but the File System could not be initialized with that address:
  AWS Access Key ID and Secret Access Key must be specified as the username or password
  (respectively) of a s3n URL, or by setting the fs.s3n.awsAccessKeyId
  or fs.s3n.awsSecretAccessKey properties (respectively) [...]
Caused by: java.lang.IllegalArgumentException: AWS Access Key ID and Secret Access Key must
  be specified as the username or password (respectively) of a s3 URL, or by setting
  the fs.s3n.awsAccessKeyId or fs.s3n.awsSecretAccessKey properties (respectively) [...]
    at o.a.h.fs.s3.S3Credentials.initialize(S3Credentials.java:70)
    at o.a.h.fs.s3native.Jets3tNativeFileSystemStore.initialize(Jets3tNativeFileSystemStore.java:80)
    at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
    at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    at java.lang.reflect.Method.invoke(Method.java:606)
    at o.a.h.io.retry.RetryInvocationHandler.invokeMethod(RetryInvocationHandler.java:187)
    at o.a.h.io.retry.RetryInvocationHandler.invoke(RetryInvocationHandler.java:102)
    at o.a.h.fs.s3native.$Proxy6.initialize(Unknown Source)
    at o.a.h.fs.s3native.NativeS3FileSystem.initialize(NativeS3FileSystem.java:330)
    at o.a.f.runtime.fs.hdfs.HadoopFileSystem.initialize(HadoopFileSystem.java:321)
{% endhighlight %}

{% top %}

### ClassNotFoundException: NativeS3FileSystem/S3AFileSystem Not Found

If you see this Exception, the S3 FileSystem is not part of the class path of Flink. Please refer to [S3 FileSystem dependency section](#provide-s3-filesystem-dependency) for details on how to configure this properly.

{% highlight plain %}
Caused by: java.lang.RuntimeException: java.lang.RuntimeException: java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3native.NativeS3FileSystem not found
  at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2186)
  at org.apache.flink.runtime.fs.hdfs.HadoopFileSystem.getHadoopWrapperClassNameForFileSystem(HadoopFileSystem.java:460)
  at org.apache.flink.core.fs.FileSystem.getHadoopWrapperClassNameForFileSystem(FileSystem.java:352)
  at org.apache.flink.core.fs.FileSystem.get(FileSystem.java:280)
  at org.apache.flink.core.fs.Path.getFileSystem(Path.java:311)
  at org.apache.flink.api.common.io.FileInputFormat.createInputSplits(FileInputFormat.java:450)
  at org.apache.flink.api.common.io.FileInputFormat.createInputSplits(FileInputFormat.java:57)
  at org.apache.flink.runtime.executiongraph.ExecutionJobVertex.<init>(ExecutionJobVertex.java:156)
  ... 25 more
Caused by: java.lang.RuntimeException: java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3native.NativeS3FileSystem not found
  at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2154)
  at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2178)
  ... 32 more
Caused by: java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3native.NativeS3FileSystem not found
  at org.apache.hadoop.conf.Configuration.getClassByName(Configuration.java:2060)
  at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2152)
  ... 33 more
{% endhighlight %}

{% top %}

### IOException: `400: Bad Request`

If you have configured everything properly, but get a `Bad Request` Exception **and** your S3 bucket is located in region `eu-central-1`, you might be running an S3 client, which does not support [Amazon's signature version 4](http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html).

{% highlight plain %}
[...]
Caused by: java.io.IOException: s3://<bucket-in-eu-central-1>/<endpoint> : 400 : Bad Request [...]
Caused by: org.jets3t.service.impl.rest.HttpException [...]
{% endhighlight %}
or
{% highlight plain %}
com.amazonaws.services.s3.model.AmazonS3Exception: Status Code: 400, AWS Service: Amazon S3, AWS Request ID: [...], AWS Error Code: null, AWS Error Message: Bad Request, S3 Extended Request ID: [...]

{% endhighlight %}

This should not apply to our shaded Hadoop/Presto S3 file systems but can occur for Hadoop-provided
S3 file systems. In particular, all Hadoop versions up to 2.7.2 running `NativeS3FileSystem` (which
depend on `JetS3t 0.9.0` instead of a version [>= 0.9.4](http://www.jets3t.org/RELEASE_NOTES.html))
are affected but users also reported this happening with the `S3AFileSystem`.

Except for changing the bucket region, you may also be able to solve this by
[requesting signature version 4 for request authentication](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingAWSSDK.html#specify-signature-version),
e.g. by adding this to Flink's JVM options in `flink-conf.yaml` (see
[configuration](../config.html#common-options)):
{% highlight yaml %}
env.java.opts: -Dcom.amazonaws.services.s3.enableV4
{% endhighlight %}

{% top %}

### NullPointerException at org.apache.hadoop.fs.LocalDirAllocator

This Exception is usually caused by skipping the local buffer directory configuration `fs.s3a.buffer.dir` for the `S3AFileSystem`. Please refer to the [S3AFileSystem configuration](#s3afilesystem-recommended) section to see how to configure the `S3AFileSystem` properly.

{% highlight plain %}
[...]
Caused by: java.lang.NullPointerException at
o.a.h.fs.LocalDirAllocator$AllocatorPerContext.confChanged(LocalDirAllocator.java:268) at
o.a.h.fs.LocalDirAllocator$AllocatorPerContext.getLocalPathForWrite(LocalDirAllocator.java:344) at
o.a.h.fs.LocalDirAllocator$AllocatorPerContext.createTmpFileForWrite(LocalDirAllocator.java:416) at
o.a.h.fs.LocalDirAllocator.createTmpFileForWrite(LocalDirAllocator.java:198) at
o.a.h.fs.s3a.S3AOutputStream.<init>(S3AOutputStream.java:87) at
o.a.h.fs.s3a.S3AFileSystem.create(S3AFileSystem.java:410) at
o.a.h.fs.FileSystem.create(FileSystem.java:907) at
o.a.h.fs.FileSystem.create(FileSystem.java:888) at
o.a.h.fs.FileSystem.create(FileSystem.java:785) at
o.a.f.runtime.fs.hdfs.HadoopFileSystem.create(HadoopFileSystem.java:404) at
o.a.f.runtime.fs.hdfs.HadoopFileSystem.create(HadoopFileSystem.java:48) at
... 25 more
{% endhighlight %}

{% top %}
