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

[Google Cloud storage](https://cloud.google.com/storage) (GCS) provides cloud storage for a variety of use cases. You can use it for **reading** and **writing data**.



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

```

### Libraries

You must include the following libraries in Flink's `lib` directory to connect Flink with gcs:

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

We have tested with `flink-shared-hadoop2-uber version` >= `2.8.3-1.8.3`.

### Authentication to access GCS

Most operations on GCS require authentication. Please see [the documentation on Google Cloud Storage authentication](https://cloud.google.com/storage/docs/authentication) for more information.

You can provide the necessary key via the `GOOGLE_APPLICATION_CREDENTIALS` environment variable. 

{{< top >}}
