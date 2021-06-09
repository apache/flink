---
title: Google Cloud Storage
weight: 3
type: docs
aliases:
  - /deployment/filesystems/gcs.html
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

# Google Cloud Storage

[Google Cloud storage](https://cloud.google.com/storage) (GCS) provides cloud storage for a variety of use cases. You can use it for **reading** and **writing data**, and for checkpoint storage when using [`FileSystemCheckpointStorage`]({{< ref "docs/ops/state/checkpoints" >}}#the-filesystemcheckpointstorage)) with the [streaming **state backends**]({{< ref "docs/ops/state/state_backends" >}}).



You can use GCS objects like regular files by specifying paths in the following format:

```plain
gs://<your-bucket>/<endpoint>
```

The endpoint can either be a single file or a directory, for example:

```java
// Read from GSC bucket
env.readTextFile("gs://<bucket>/<endpoint>");

// Write to GCS bucket
stream.writeAsText("gs://<bucket>/<endpoint>");

// Use GCS as checkpoint storage
env.getCheckpointConfig().setCheckpointStorage("gs://<bucket>/<endpoint>");

```

### Libraries

You must include the following jars in Flink's `lib` directory to connect Flink with gcs:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-shaded-hadoop2-uber</artifactId>
  <version>${flink.shared_hadoop_latest_version}</version>
</dependency>

<dependency>
  <groupId>com.google.cloud.bigdataoss</groupId>
  <artifactId>gcs-connector</artifactId>
  <version>hadoop2-2.2.0</version>
</dependency>
```

We have tested with `flink-shared-hadoop2-uber` version >= `2.8.3-1.8.3`.
You can track the latest version of the [gcs-connector hadoop 2](https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar).

### Authentication to access GCS

Most operations on GCS require authentication. Please see [the documentation on Google Cloud Storage authentication](https://cloud.google.com/storage/docs/authentication) for more information.

You can use the following method for authentication
* Configure via core-site.xml
  You would need to add the following properties to `core-site.xml`

  ```xml
  <configuration>
    <property>
      <name>google.cloud.auth.service.account.enable</name>
      <value>true</value>
    </property>
    <property>
      <name>google.cloud.auth.service.account.json.keyfile</name>
      <value><PATH TO GOOGLE AUTHENTICATION JSON></value>
    </property>
  </configuration>
  ```

  You would need to add the following to `flink-conf.yaml`

  ```yaml
  flinkConfiguration:
    fs.hdfs.hadoopconf: <DIRECTORY PATH WHERE core-site.xml IS SAVED>
  ```

* You can provide the necessary key via the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.



{{< top >}}
