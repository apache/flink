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

Flink supports accessing Azure Blob Storage using both [wasb://](https://hadoop.apache.org/docs/stable/hadoop-azure/index.html) or [abfs://](https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html).

{{< hint info >}} 
Azure recommends using abfs:// for accessing ADLS Gen2 storage accounts even though wasb:// works through backward compatibility.  
{{< /hint >}}

{{< hint warning >}}
abfs:// can be used for accessing the ADLS Gen2 storage accounts only. Please visit Azure documentation on how to identify ADLS Gen2 storage account.  
{{< /hint >}}


You can use Azure Blob Storage objects like regular files by specifying paths in the following format:

```plain
// WASB unencrypted access
wasb://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>

// WASB SSL encrypted access
wasbs://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>

// ABFS unecrypted access
abfs://<your-container>@$<your-azure-account>.dfs.core.windows.net/<object-path>

// ABFS SSL encrypted access
abfss://<your-container>@$<your-azure-account>.dfs.core.windows.net/<object-path>
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

## Credentials Configuration

### WASB

Hadoop's WASB Azure Filesystem supports configuration of credentials via the Hadoop configuration as 
outlined in the [Hadoop Azure Blob Storage documentation](https://hadoop.apache.org/docs/current/hadoop-azure/index.html#Configuring_Credentials).
For convenience Flink forwards all Flink configurations with a key prefix of `fs.azure` to the 
Hadoop configuration of the filesystem. Consequently, the azure blob storage key can be configured 
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

### ABFS

Hadoop's ABFS Azure Filesystem supports several ways of configuring authentication. Please visit the [Hadoop ABFS documentation](https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html#Authentication) documentation on how to configure.

{{< hint info >}}
Azure recommends using Azure managed identity to access the ADLS Gen2 storage accounts using abfs. Please refer to [Azure managed identities documentation](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/) for more details.

Please visit the [page](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/services-support-managed-identities#azure-services-that-support-managed-identities-for-azure-resources) for the list of services that support Managed Identities. Flink clusters deployed in those Azure services can take advantage of Managed Identities.
{{< /hint >}}

##### Accessing ABFS using storage Keys (Discouraged)
Azure blob storage key can be configured in `flink-conf.yaml` via:

```yaml
fs.azure.account.key.<account_name>.dfs.core.windows.net: <azure_storage_key>
```

{{< top >}}
