---
title: "Azure Blob Storage"
nav-title: Azure Blob Storage
nav-parent_id: filesystems
nav-pos: 3
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

[Azure Blob Storage](https://docs.microsoft.com/en-us/azure/storage/) is a Microsoft-managed service providing cloud storage for a variety of use cases.
You can use Azure Blob Storage with Flink for **reading** and **writing data** as well in conjunction with the [streaming **state backends**]({{ site.baseurl }}/ops/state/state_backends.html)  

* This will be replaced by the TOC
{:toc}

You can use Azure Blob Storage objects like regular files by specifying paths in the following format:

{% highlight plain %}
wasb://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>

// SSL encrypted access
wasbs://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>
{% endhighlight %}

See below for how to use Azure Blob Storage in a Flink job:

{% highlight java %}
// Read from Azure Blob storage
env.readTextFile("wasb://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>");

// Write to Azure Blob storage
stream.writeAsText("wasb://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>")

// Use Azure Blob Storage as FsStatebackend
env.setStateBackend(new FsStateBackend("wasb://<your-container>@$<your-azure-account>.blob.core.windows.net/<object-path>"));
{% endhighlight %}

### Shaded Hadoop Azure Blob Storage file system

To use `flink-azure-fs-hadoop,` copy the respective JAR file from the `opt` directory to the `plugins` directory of your Flink distribution before starting Flink, e.g.

{% highlight bash %}
mkdir ./plugins/azure-fs-hadoop
cp ./opt/flink-azure-fs-hadoop-{{ site.version }}.jar ./plugins/azure-fs-hadoop/
{% endhighlight %}

`flink-azure-fs-hadoop` registers default FileSystem wrappers for URIs with the *wasb://* and *wasbs://* (SSL encrypted access) scheme.

#### Configurations setup
After setting up the Azure Blob Storage FileSystem wrapper, you need to configure credentials to make sure that Flink is allowed to access Azure Blob Storage.

To allow for easy adoption, you can use the same configuration keys in `flink-conf.yaml` as in Hadoop's `core-site.xml`

You can see the configuration keys in the [Hadoop Azure Blob Storage documentation](https://hadoop.apache.org/docs/current/hadoop-azure/index.html#Configuring_Credentials).

There are some required configurations that must be added to `flink-conf.yaml`:

{% highlight yaml %}
fs.azure.account.key.youraccount.blob.core.windows.net: Azure Blob Storage access key
{% endhighlight %}

{% top %}
