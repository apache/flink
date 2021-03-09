---
title: Aliyun OSS
weight: 4
type: docs
aliases:
  - /deployment/filesystems/oss.html
  - /ops/filesystems/oss.html
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

# Aliyun Object Storage Service (OSS)

## OSS: Object Storage Service

[Aliyun Object Storage Service](https://www.aliyun.com/product/oss) (Aliyun OSS) is widely used, particularly popular among China’s cloud users, and it provides cloud object storage for a variety of use cases.
You can use OSS with Flink for **reading** and **writing data** as well in conjunction with the [streaming **state backends**]({{< ref "docs/ops/state/state_backends" >}})



You can use OSS objects like regular files by specifying paths in the following format:

```plain
oss://<your-bucket>/<object-name>
```

Below shows how to use OSS in a Flink job:

```java
// Read from OSS bucket
env.readTextFile("oss://<your-bucket>/<object-name>");

// Write to OSS bucket
stream.writeAsText("oss://<your-bucket>/<object-name>")

// Use OSS as checkpoint storage
env.getCheckpointConfig().setCheckpointStorage("oss://<your-bucket>/<object-name>");
```

### Shaded Hadoop OSS file system

To use `flink-oss-fs-hadoop`, copy the respective JAR file from the `opt` directory to a directory in `plugins` directory of your Flink distribution before starting Flink, e.g.

```bash
mkdir ./plugins/oss-fs-hadoop
cp ./opt/flink-oss-fs-hadoop-{{< version >}}.jar ./plugins/oss-fs-hadoop/
```

`flink-oss-fs-hadoop` registers default FileSystem wrappers for URIs with the *oss://* scheme.

#### Configurations setup

After setting up the OSS FileSystem wrapper, you need to add some configurations to make sure that Flink is allowed to access your OSS buckets.

To allow for easy adoption, you can use the same configuration keys in `flink-conf.yaml` as in Hadoop's `core-site.xml`

You can see the configuration keys in the [Hadoop OSS documentation](http://hadoop.apache.org/docs/current/hadoop-aliyun/tools/hadoop-aliyun/index.html).

There are some required configurations that must be added to `flink-conf.yaml` (**Other configurations defined in Hadoop OSS documentation are advanced configurations which used by performance tuning**):

```yaml
fs.oss.endpoint: Aliyun OSS endpoint to connect to
fs.oss.accessKeyId: Aliyun access key ID
fs.oss.accessKeySecret: Aliyun access key secret
```

An alternative `CredentialsProvider` can also be configured in the `flink-conf.yaml`, e.g. 
```yaml
# Read Credentials from OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET
fs.oss.credentials.provider: com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider
```
Other credential providers can be found under [here](https://github.com/aliyun/aliyun-oss-java-sdk/tree/master/src/main/java/com/aliyun/oss/common/auth).

 

{{< top >}}
