---
title: "Plugins"
weight: 5
type: docs
aliases:
  - /deployment/filesystems/plugins.html
  - /ops/plugins.html
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

# Plugins

Plugins facilitate a strict separation of code through restricted classloaders. Plugins cannot
access classes from other plugins or from Flink that have not been specifically whitelisted. This
strict isolation allows plugins to contain conflicting versions of the same library without the need
to relocate classes or to converge to common versions. Currently, file systems and metric reporters are pluggable
but in the future, connectors, formats, and even user code should also be pluggable.



## Isolation and plugin structure

Plugins reside in their own folders and can consist of several jars. The names of the plugin folders
are arbitrary.

```
flink-dist
├── conf
├── lib
...
└── plugins
    ├── s3
    │   ├── aws-credential-provider.jar
    │   └── flink-s3-fs-hadoop.jar
    └── azure
        └── flink-azure-fs-hadoop.jar
``` 

Each plugin is loaded through its own classloader and completely isolated from any other plugin.
Hence, the `flink-s3-fs-hadoop` and `flink-azure-fs-hadoop` can depend on different conflicting
library versions. There is no need to relocate any class during the creation of fat jars (shading).

Plugins may access certain whitelisted packages from Flink's `lib/` folder. In particular, all
necessary service provider interfaces (SPI) are loaded through the system classloader, so that no
two versions of `org.apache.flink.core.fs.FileSystem` exist at any given time, even if users
accidentally bundle it in their fat jar. This singleton class requirement is strictly necessary so
that the Flink runtime has an entry point into the plugin. Service classes are discovered through
the `java.util.ServiceLoader`, so make sure to retain the service definitions in `META-INF/services`
during shading.

<span class="label label-warning">Note</span> *Currently, more Flink core classes are still
accessible from plugins as we flesh out the SPI system.*

Furthermore, the most common logger frameworks are whitelisted, such that logging is uniformly
possible across Flink core, plugins, and user code.

## File Systems

All [file systems]({{< ref "docs/deployment/filesystems/overview" >}}) **except MapR** are pluggable. That means they can and should
be used as plugins. To use a pluggable file system, copy the corresponding JAR file from the `opt`
directory to a directory under `plugins` directory of your Flink distribution before starting Flink,
e.g.

```bash
mkdir ./plugins/s3-fs-hadoop
cp ./opt/flink-s3-fs-hadoop-{{< version >}}.jar ./plugins/s3-fs-hadoop/
```

{{< hint warning >}}
The s3 file systems (`flink-s3-fs-presto` and
`flink-s3-fs-hadoop`) can only be used as plugins as we already removed the relocations. Placing them in libs/ will result in system failures.
{{< /hint >}}

{{< hint warning >}}
Because of the [strict isolation](#isolation-and-plugin-structure), file systems do not have access to credential providers in lib/
anymore. Please add any needed providers to the respective plugin folder.
{{< /hint >}}

<!-- 
Add when we support more than just file systems.
## Implementing a new plugin

To implement a new plugin make sure you only use `@Public` and `@PublicEvolving` classes and
interfaces. Other classes will not be accessible in the future. After picking the respective SPI 
(e.g., `org.apache.flink.core.fs.FileSystem`), also add a service entry. Create a file
`META-INF/services/<SPI>` which contains the class name of your implementation class (see the [Java
Service Loader docs](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html).

During plugins discovery, the service class will be loaded by a dedicated Java class loader to avoid
class conflicts with other plugins and Flink components. The same class loader should be used during
file system instantiation and the file system operation calls.

<span class="label label-warning">Warning</span> In practice, it means you should avoid using
`Thread.currentThread().getContextClassLoader()` class loader in your implementation.
-->

<!-- 
Add when we have a real whitelist
## Whitelist

The whitelisted classes mainly consists of the necessary interfaces to implement the plugins.
Furthermore, loggers are whitelisted, so that they are configured properly.
-->

## Metric Reporters

All [metric reporters]({{< ref "docs/deployment/metric_reporters" >}}) that Flink provides can be used as plugins.
See the [metrics]({{< ref "docs/ops/metrics" >}}) documentation for more details.

{{< top >}}
