---
title: Azure Blob Storage
weight: 4
type: docs
aliases:
  - /deployment/filesystems/azure.html
  - /ops/filesystems/azure
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

# Azure Blob Storage

[Azure Blob Storage](https://docs.microsoft.com/en-us/azure/storage/) is a Microsoft-managed service providing cloud storage for a variety of use cases.
You can use Azure Blob Storage with Flink for **reading** and **writing data** as well in conjunction with the [streaming **state backends**]({{< ref "docs/ops/state/state_backends" >}})  

You can use Azure Blob Storage objects like regular files by specifying paths in the following format:

```plain
wasb://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>

// SSL encrypted access
wasbs://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>
```

See below for how to use Azure Blob Storage in a Flink job:

```java
// Read from Azure Blob storage
env.readTextFile("wasb://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>");

// Write to Azure Blob storage
stream.writeAsText("wasb://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>")

// Use Azure Blob Storage as checkpoint storage
env.getCheckpointConfig().setCheckpointStorage("wasb://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>");
```

### Shaded Hadoop Azure Blob Storage file system

To use `flink-azure-fs-hadoop`, copy the respective JAR file from the `opt` directory to the `plugins` directory of your Flink distribution before starting Flink, e.g.

```bash
mkdir ./plugins/azure-fs-hadoop
cp ./opt/flink-azure-fs-hadoop-{{< version >}}.jar ./plugins/azure-fs-hadoop/
```

`flink-azure-fs-hadoop` registers default FileSystem wrappers for URIs with the *wasb://* and *wasbs://* (SSL encrypted access) scheme.

### Credentials Configuration

Hadoop's Azure Filesystem supports configuration of credentials via the Hadoop configuration as 
outlined in the [Hadoop Azure Blob Storage documentation](https://hadoop.apache.org/docs/current/hadoop-azure/index.html#Configuring_Credentials).
For convenience Flink forwards all Flink configurations with a key prefix of `fs.azure` to the 
Hadoop configuration of the filesystem. Consequentially, the azure blob storage key can be configured 
in `flink-conf.yaml` via:

```yaml
fs.azure.account.key.<account_name>.blob.core.windows.net: <azure_storage_key>
```

Alternatively, the filesystem can be configured to read the Azure Blob Storage key from an 
environment variable `AZURE_STORAGE_KEY` by setting the following configuration keys in 
`flink-conf.yaml`.  

```yaml
fs.azure.account.keyprovider.<account_name>.blob.core.windows.net: org.apache.flink.fs.azurefs.EnvironmentVariableKeyProvider
```

{{< top >}}
