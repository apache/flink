---
title: "Frequently Asked Questions (FAQ)"
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

* This will be replaced by the TOC
{:toc}

## General

### Is Flink a Hadoop Project?

Flink is a data processing system and an alternative to Hadoop's
MapReduce component. It comes with its own runtime, rather than building on top
of MapReduce. As such, it can work completely independently of the Hadoop
ecosystem. However, Flink can also access Hadoop's distributed file
system (HDFS) to read and write data, and Hadoop's next-generation resource
manager (YARN) to provision cluster resources. Since most Flink users are
using Hadoop HDFS to store their data, we ship already the required libraries to
access HDFS.

### Do I have to install Apache Hadoop to use Flink?

No. Flink can run without a Hadoop installation. However, a very common
setup is to use Flink to analyze data stored in the Hadoop Distributed
File System (HDFS). To make these setups work out of the box, we bundle the
Hadoop client libraries with Flink by default.

Additionally, we provide a special YARN Enabled download of Flink for
users with an existing Hadoop YARN cluster. [Apache Hadoop
YARN](http://hadoop.apache.org/docs/r2.2.0/hadoop-yarn/hadoop-yarn-site/YARN.html) 
is Hadoop's cluster resource manager that allows to use
different execution engines next to each other on a cluster.

## Usage

### How do I assess the progress of a Flink program?

There are a multiple of ways to track the progress of a Flink program:

- The JobManager (the master of the distributed system) starts a web interface
to observe program execution. In runs on port 8081 by default (configured in
`conf/flink-config.yml`).
- When you start a program from the command line, it will print the status
changes of all operators as the program progresses through the operations.
- All status changes are also logged to the JobManager's log file.

### How can I figure out why a program failed?

- The JobManager web frontend (by default on port 8081) displays the exceptions
of failed tasks.
- If you run the program from the command-line, task exceptions are printed to
the standard error stream and shown on the console.
- Both the command line and the web interface allow you to figure out which
parallel task first failed and caused the other tasks to cancel the execution.
- Failing tasks and the corresponding exceptions are reported in the log files
of the master and the worker where the exception occurred
(`log/flink-<user>-jobmanager-<host>.log` and
`log/flink-<user>-taskmanager-<host>.log`).

### How do I debug Flink programs?

- When you start a program locally with the [LocalExecutor](local_execution.html),
you can place breakpoints in your functions and debug them like normal
Java/Scala programs.
- The [Accumulators](programming_guide.html#accumulators--counters) are very helpful in
tracking the behavior of the parallel execution. They allow you to gather
information inside the program's operations and show them after the program
execution.

## Errors

### Why am I getting a "NonSerializableException" ?

All functions in Flink must be serializable, as defined by [java.io.Serializable](http://docs.oracle.com/javase/7/docs/api/java/io/Serializable.html).
Since all function interfaces are serializable, the exception means that one
of the fields used in your function is not serializable.

In particular, if your function is an inner class, or anonymous inner class,
it contains a hidden reference to the enclosing class (usually called `this$0`, if you look
at the function in the debugger). If the enclosing class is not serializable, this is probably
the source of the error. Solutions are to

- make the function a standalone class, or a static inner class (no more reference to the enclosing class)
- make the enclosing class serializable
- use a Java 8 lambda function.

### I get an error message saying that not enough buffers are available. How do I fix this?

If you run Flink in a massively parallel setting (100+ parallel threads),
you need to adapt the number of network buffers via the config parameter
`taskmanager.network.numberOfBuffers`.
As a rule-of-thumb, the number of buffers should be at least
`4 * numberOfNodes * numberOfTasksPerNode^2`. See
[Configuration Reference](config.html) for details.

### My job fails early with a java.io.EOFException. What could be the cause?

The most common case for these exception is when Flink is set up with the
wrong HDFS version. Because different HDFS versions are often not compatible
with each other, the connection between the filesystem master and the client
breaks.

~~~bash
Call to <host:port> failed on local exception: java.io.EOFException
    at org.apache.hadoop.ipc.Client.wrapException(Client.java:775)
    at org.apache.hadoop.ipc.Client.call(Client.java:743)
    at org.apache.hadoop.ipc.RPC$Invoker.invoke(RPC.java:220)
    at $Proxy0.getProtocolVersion(Unknown Source)
    at org.apache.hadoop.ipc.RPC.getProxy(RPC.java:359)
    at org.apache.hadoop.hdfs.DFSClient.createRPCNamenode(DFSClient.java:106)
    at org.apache.hadoop.hdfs.DFSClient.<init>(DFSClient.java:207)
    at org.apache.hadoop.hdfs.DFSClient.<init>(DFSClient.java:170)
    at org.apache.hadoop.hdfs.DistributedFileSystem.initialize(DistributedFileSystem.java:82)
    at org.apache.flinkruntime.fs.hdfs.DistributedFileSystem.initialize(DistributedFileSystem.java:276
~~~

Please refer to the [download page]({{site.baseurl}}/downloads.html#maven) and
the {% gh_link README.md master "build instructions" %}
for details on how to set up Flink for different Hadoop and HDFS versions.

### In Eclipse, I get compilation errors in the Scala projects

Flink uses a new feature of the Scala compiler (called "quasiquotes") that have not yet been properly
integrated with the Eclipse Scala plugin. In order to make this feature available in Eclipse, you
need to manually configure the *flink-scala* project to use a *compiler plugin*:

- Right click on *flink-scala* and choose "Properties"
- Select "Scala Compiler" and click on the "Advanced" tab. (If you do not have that, you probably have not set up Eclipse for Scala properly.)
- Check the box "Use Project Settings"
- In the field "Xplugin", put the path "/home/<user-name>/.m2/repository/org/scalamacros/paradise_2.10.4/2.0.1/paradise_2.10.4-2.0.1.jar"
- NOTE: You have to build Flink with Maven on the command line first, to make sure the plugin is downloaded.

### My program does not compute the correct result. Why are my custom key types
are not grouped/joined correctly?

Keys must correctly implement the methods `java.lang.Object#hashCode()`,
`java.lang.Object#equals(Object o)`, and `java.util.Comparable#compareTo(...)`.
These methods are always backed with default implementations which are usually
inadequate. Therefore, all keys must override `hashCode()` and `equals(Object o)`.

### I get a java.lang.InstantiationException for my data type, what is wrong?

All data type classes must be public and have a public nullary constructor
(constructor with no arguments). Further more, the classes must not be abstract
or interfaces. If the classes are internal classes, they must be public and
static.

### I can't stop Flink with the provided stop-scripts. What can I do?

Stopping the processes sometimes takes a few seconds, because the shutdown may
do some cleanup work.

In some error cases it happens that the JobManager or TaskManager cannot be
stopped with the provided stop-scripts (`bin/stop-local.sh` or `bin/stop-
cluster.sh`). You can kill their processes on Linux/Mac as follows:

- Determine the process id (pid) of the JobManager / TaskManager process. You
can use the `jps` command on Linux(if you have OpenJDK installed) or command
`ps -ef | grep java` to find all Java processes. 
- Kill the process with `kill -9 <pid>`, where `pid` is the process id of the
affected JobManager or TaskManager process.
    
On Windows, the TaskManager shows a table of all processes and allows you to
destroy a process by right its entry.

### I got an OutOfMemoryException. What can I do?

These exceptions occur usually when the functions in the program consume a lot
of memory by collection large numbers of objects, for example in lists or maps.
The OutOfMemoryExceptions in Java are kind of tricky. The exception is not
necessarily thrown by the component that allocated most of the memory but by the
component that tried to requested the latest bit of memory that could not be
provided.

There are two ways to go about this:

1. See whether you can use less memory inside the functions. For example, use
arrays of primitive types instead of object types.

2. Reduce the memory that Flink reserves for its own processing. The
TaskManager reserves a certain portion of the available memory for sorting,
hashing, caching, network buffering, etc. That part of the memory is unavailable
to the user-defined functions. By reserving it, the system can guarantee to not
run out of memory on large inputs, but to plan with the available memory and
destage operations to disk, if necessary. By default, the system reserves around
70% of the memory. If you frequently run applications that need more memory in
the user-defined functions, you can reduce that value using the configuration
entries `taskmanager.memory.fraction` or `taskmanager.memory.size`. See the
[Configuration Reference](config.html) for details. This will leave more memory to JVM heap,
but may cause data processing tasks to go to disk more often.

### Why do the TaskManager log files become so huge?

Check the logging behavior of your jobs. Emitting logging per or tuple may be
helpful to debug jobs in small setups with tiny data sets, it becomes very
inefficient and disk space consuming if used for large input data.

## YARN Deployment

### The YARN session runs only for a few seconds

The `./bin/yarn-session.sh` script is intended to run while the YARN-session is
open. In some error cases however, the script immediately stops running. The
output looks like this:

~~~
07:34:27,004 INFO  org.apache.hadoop.yarn.client.api.impl.YarnClientImpl         - Submitted application application_1395604279745_273123 to ResourceManager at jobtracker-host
Flink JobManager is now running on worker1:6123
JobManager Web Interface: http://jobtracker-host:54311/proxy/application_1295604279745_273123/
07:34:51,528 INFO  org.apache.flinkyarn.Client                                   - Application application_1295604279745_273123 finished with state FINISHED at 1398152089553
07:34:51,529 INFO  org.apache.flinkyarn.Client                                   - Killing the Flink-YARN application.
07:34:51,529 INFO  org.apache.hadoop.yarn.client.api.impl.YarnClientImpl         - Killing application application_1295604279745_273123
07:34:51,534 INFO  org.apache.flinkyarn.Client                                   - Deleting files in hdfs://user/marcus/.flink/application_1295604279745_273123
07:34:51,559 INFO  org.apache.flinkyarn.Client                                   - YARN Client is shutting down
~~~

The problem here is that the Application Master (AM) is stopping and the YARN client assumes that the application has finished.

There are three possible reasons for that behavior:

- The ApplicationMaster exited with an exception. To debug that error, have a
look in the logfiles of the container. The `yarn-site.xml` file contains the
configured path. The key for the path is `yarn.nodemanager.log-dirs`, the
default value is `${yarn.log.dir}/userlogs`.

- YARN has killed the container that runs the ApplicationMaster. This case
happens when the AM used too much memory or other resources beyond YARN's
limits. In this case, you'll find error messages in the nodemanager logs on
the host.

- The operating system has shut down the JVM of the AM. This can happen if the
YARN configuration is wrong and more memory than physically available is
configured. Execute `dmesg` on the machine where the AM was running to see if
this happened. You see messages from Linux' [OOM killer](http://linux-mm.org/OOM_Killer).

### The YARN session crashes with a HDFS permission exception during startup

While starting the YARN session, you are receiving an exception like this:

~~~
Exception in thread "main" org.apache.hadoop.security.AccessControlException: Permission denied: user=robert, access=WRITE, inode="/user/robert":hdfs:supergroup:drwxr-xr-x
  at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.check(FSPermissionChecker.java:234)
  at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.check(FSPermissionChecker.java:214)
  at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.checkPermission(FSPermissionChecker.java:158)
  at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.checkPermission(FSNamesystem.java:5193)
  at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.checkPermission(FSNamesystem.java:5175)
  at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.checkAncestorAccess(FSNamesystem.java:5149)
  at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.startFileInternal(FSNamesystem.java:2090)
  at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.startFileInt(FSNamesystem.java:2043)
  at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.startFile(FSNamesystem.java:1996)
  at org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer.create(NameNodeRpcServer.java:491)
  at org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB.create(ClientNamenodeProtocolServerSideTranslatorPB.java:301)
  at org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos$ClientNamenodeProtocol$2.callBlockingMethod(ClientNamenodeProtocolProtos.java:59570)
  at org.apache.hadoop.ipc.ProtobufRpcEngine$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine.java:585)
  at org.apache.hadoop.ipc.RPC$Server.call(RPC.java:928)
  at org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2053)
  at org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2049)
  at java.security.AccessController.doPrivileged(Native Method)
  at javax.security.auth.Subject.doAs(Subject.java:396)
  at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1491)
  at org.apache.hadoop.ipc.Server$Handler.run(Server.java:2047)

  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:39)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:27)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:513)
  at org.apache.hadoop.ipc.RemoteException.instantiateException(RemoteException.java:106)
  at org.apache.hadoop.ipc.RemoteException.unwrapRemoteException(RemoteException.java:73)
  at org.apache.hadoop.hdfs.DFSOutputStream.newStreamForCreate(DFSOutputStream.java:1393)
  at org.apache.hadoop.hdfs.DFSClient.create(DFSClient.java:1382)
  at org.apache.hadoop.hdfs.DFSClient.create(DFSClient.java:1307)
  at org.apache.hadoop.hdfs.DistributedFileSystem$6.doCall(DistributedFileSystem.java:384)
  at org.apache.hadoop.hdfs.DistributedFileSystem$6.doCall(DistributedFileSystem.java:380)
  at org.apache.hadoop.fs.FileSystemLinkResolver.resolve(FileSystemLinkResolver.java:81)
  at org.apache.hadoop.hdfs.DistributedFileSystem.create(DistributedFileSystem.java:380)
  at org.apache.hadoop.hdfs.DistributedFileSystem.create(DistributedFileSystem.java:324)
  at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:905)
  at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:886)
  at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:783)
  at org.apache.hadoop.fs.FileUtil.copy(FileUtil.java:365)
  at org.apache.hadoop.fs.FileUtil.copy(FileUtil.java:338)
  at org.apache.hadoop.fs.FileSystem.copyFromLocalFile(FileSystem.java:2021)
  at org.apache.hadoop.fs.FileSystem.copyFromLocalFile(FileSystem.java:1989)
  at org.apache.hadoop.fs.FileSystem.copyFromLocalFile(FileSystem.java:1954)
  at org.apache.flinkyarn.Utils.setupLocalResource(Utils.java:176)
  at org.apache.flinkyarn.Client.run(Client.java:362)
  at org.apache.flinkyarn.Client.main(Client.java:568)
~~~

The reason for this error is, that the home directory of the user **in HDFS**
has the wrong permissions. The user (in this case `robert`) can not create
directories in his own home directory.

Flink creates a `.flink/` directory in the users home directory
where it stores the Flink jar and configuration file.

## Features

### What kind of fault-tolerance does Flink provide?

Flink can restart failed jobs. Mid-query fault tolerance will go into the
open source project in the next versions.

### Are Hadoop-like utilities, such as Counters and the DistributedCache supported?

[Flink's Accumulators](programming_guide.html#accumulators--counters) work very similar like
[Hadoop's counters, but are more powerful.

Flink has a {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/cache/DistributedCache.java "Distributed Cache" %} that is deeply integrated with the APIs. Please refer to the {% gh_link /flink-java/src/main/java/org/apache/flink/api/java/ExecutionEnvironment.java#L561 "JavaDocs" %} for details on how to use it.

In order to make data sets available on all tasks, we encourage you to use [Broadcast Variables](programming_guide.html#broadcast-variables) instead. They are more efficient and easier to use than the distributed cache.

