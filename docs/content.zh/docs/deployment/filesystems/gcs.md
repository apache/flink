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

[Google Cloud Storage](https://cloud.google.com/storage) (GCS) provides cloud storage for a variety of use cases. You can use it for **reading** and **writing** data, and for checkpoint storage when using [`FileSystemCheckpointStorage`]({{< ref "docs/ops/state/checkpoints" >}}#the-filesystemcheckpointstorage)) with the [streaming **state backends**]({{< ref "docs/ops/state/state_backends" >}}).

You can use GCS objects like regular files by specifying paths in the following format:

```plain
gs://<your-bucket>/<endpoint>
```

The endpoint can either be a single file or a directory, for example:

```java
// Read from GCS bucket
env.readTextFile("gs://<bucket>/<endpoint>");

// Write to GCS bucket
stream.writeAsText("gs://<bucket>/<endpoint>");

// Use GCS as checkpoint storage
env.getCheckpointConfig().setCheckpointStorage("gs://<bucket>/<endpoint>");

```

Note that these examples are *not* exhaustive and you can use GCS in other places as well, including your [high availability setup]({{< ref "docs/deployment/ha/overview" >}}) or the [EmbeddedRocksDBStateBackend]({{< ref "docs/ops/state/state_backends" >}}#the-rocksdbstatebackend); everywhere that Flink expects a FileSystem URI.

### GCS File System plugin

Flink provides the `flink-gs-fs-hadoop` file system to write to GCS.
This implementation is self-contained with no dependency footprint, so there is no need to add Hadoop to the classpath to use it.

`flink-gs-fs-hadoop` registers a `FileSystem` wrapper for URIs with the *gs://* scheme. It uses Google's [gcs-connector](https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector/hadoop3-2.2.15) Hadoop library to access GCS. It also uses Google's [google-cloud-storage](https://mvnrepository.com/artifact/com.google.cloud/google-cloud-storage/2.15.0) library to provide `RecoverableWriter` support.

This file system can be used with the [FileSystem connector]({{< ref "docs/connectors/datastream/filesystem.md" >}}).

To use `flink-gs-fs-hadoop`, copy the JAR file from the `opt` directory to the `plugins` directory of your Flink distribution before starting Flink, i.e.

```bash
mkdir ./plugins/gs-fs-hadoop
cp ./opt/flink-gs-fs-hadoop-{{< version >}}.jar ./plugins/gs-fs-hadoop/
```

### Configuration

The underlying Hadoop file system can be configured using the [Hadoop configuration keys](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/v2.2.15/gcs/CONFIGURATION.md) for `gcs-connector` by adding the configurations to your `flink-conf.yaml`.

For example, `gcs-connector` has a `fs.gs.http.connect-timeout` configuration key. If you want to change it, you need to set `gs.http.connect-timeout: xyz` in `flink-conf.yaml`. Flink will internally translate this back to `fs.gs.http.connect-timeout`.

You can also set `gcs-connector` options directly in the Hadoop `core-site.xml` configuration file, so long as the Hadoop configuration directory is made known to Flink via the `env.hadoop.conf.dir` Flink option or via the `HADOOP_CONF_DIR` environment variable.

`flink-gs-fs-hadoop` can also be configured by setting the following options in `flink-conf.yaml`:

| Key                                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| gs.writer.temporary.bucket.name           | Set this property to choose a bucket to hold temporary blobs for in-progress writes via `RecoverableWriter`. If this property is not set, temporary blobs will be written to same bucket as the final file being written. In either case, temporary blobs are written with the prefix `.inprogress/`. <br><br>  It is recommended to choose a separate bucket in order to [assign it a TTL](https://cloud.google.com/storage/docs/lifecycle), to provide a mechanism to clean up orphaned blobs that can occur when restoring from check/savepoints.<br><br>If you do use a separate bucket with a TTL for temporary blobs, attempts to restart jobs from check/savepoints after the TTL interval expires may fail. 
| gs.writer.chunk.size                      | Set this property to [set the chunk size](https://cloud.google.com/java/docs/reference/google-cloud-core/latest/com.google.cloud.WriteChannel#com_google_cloud_WriteChannel_setChunkSize_int_) for writes via `RecoverableWriter`. <br><br>If not set, a Google-determined default chunk size will be used.                                                                                                                                                                                                                                                                                                                                                                     |

### Authentication to access GCS

Most operations on GCS require authentication. To provide authentication credentials, either:

* Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of the JSON credentials file, as described [here](https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable), where JobManagers and TaskManagers run. This is the **recommended** method.


* Set the `google.cloud.auth.service.account.json.keyfile` property in `core-site.xml` to the path to the JSON credentials file (and make sure that the Hadoop configuration directory is specified to Flink as described [above](#configuration)):

```
<configuration>
  <property>
    <name>google.cloud.auth.service.account.json.keyfile</name>
    <value>PATH TO GOOGLE AUTHENTICATION JSON FILE</value>
  </property>
</configuration>
```

For `flink-gs-fs-hadoop` to use credentials via either of these two methods, the use of service accounts for authentication must be enabled. This is enabled by default; however, it can be disabled in `core-site.xml` by setting:

```
<configuration>
  <property>
    <name>google.cloud.auth.service.account.enable</name>
    <value>false</value>
  </property>
</configuration>
```

{{< hint warning >}}
`gcs-connector` supports additional options to provide authentication credentials besides the `google.cloud.auth.service.account.json.keyfile` option described above.

However, if you use any of those other options, the provided credentials will not be used by the `google-cloud-storage` library, which provides `RecoverableWriter` support, so Flink recoverable-write operations would be expected to fail.

For this reason, use of the `gcs-connector` authentication-credentials options other than `google.cloud.auth.service.account.json.keyfile` is **not recommended.**


{{< /hint >}}


{{< top >}}
