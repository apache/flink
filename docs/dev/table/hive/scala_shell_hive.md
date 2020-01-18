---
title: "Use Hive connector in scala shell"
nav-parent_id: hive_tableapi
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

NOTE: since blink planner is not well supported in Scala Shell at the moment, it's **NOT** recommended to use Hive connector in Scala Shell.

[Flink Scala Shell]({{ site.baseurl }}/ops/scala_shell.html) is a convenient quick way to try flink. 
You can use hive in scala shell as well instead of specifying hive dependencies in pom file, packaging your program and submitting it via flink run command.
In order to use hive connector in scala shell, you need to put the following [hive connector dependencies]({{ site.baseurl }}/dev/table/hive/#depedencies) under lib folder of flink dist .

* flink-connector-hive_{scala_version}-{flink.version}.jar
* flink-hadoop-compatibility_{scala_version}-{flink.version}.jar
* flink-shaded-hadoop-2-uber-{hadoop.version}-{flink-shaded.version}.jar
* hive-exec-2.x.jar (for Hive 1.x, you need to copy hive-exec-1.x.jar, hive-metastore-1.x.jar, libfb303-0.9.2.jar and libthrift-0.9.2.jar)

Then you can use hive connector in scala shell like following:

{% highlight scala %}
Scala-Flink> import org.apache.flink.table.catalog.hive.HiveCatalog
Scala-Flink> val hiveCatalog = new HiveCatalog("hive", "default", "<Replace it with HIVE_CONF_DIR>", "2.3.4");
Scala-Flink> btenv.registerCatalog("hive", hiveCatalog)
Scala-Flink> btenv.useCatalog("hive")
Scala-Flink> btenv.listTables
Scala-Flink> btenv.sqlQuery("<sql query>").toDataSet[Row].print()
{% endhighlight %}
