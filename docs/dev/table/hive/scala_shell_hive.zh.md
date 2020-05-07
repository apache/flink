---
title: "在 Scala Shell 中使用 Hive 连接器"
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

注意：目前 blink planner 还不能很好的支持 Scala Shell，因此 **不** 建议在 Scala Shell 中使用 Hive 连接器。

[Flink Scala Shell]({{ site.baseurl }}/zh/ops/scala_shell.html) 是快速上手 Flink 的好方法。
你可以在 Scala Shell 中直接使用 Hive 连接器，而不需要在 pom 中引入 Hive 相关依赖，并打包提交作业。
想要在 Scala Shell 中使用 Hive 连接器，你需要把 [Hive 连接器依赖项]({{ site.baseurl }}/zh/dev/table/hive/#depedencies) 放在 Flink dist 包中的 lib 文件夹下。

* flink-connector-hive_{scala_version}-{flink.version}.jar
* flink-hadoop-compatibility_{scala_version}-{flink.version}.jar
* flink-shaded-hadoop-2-uber-{hadoop.version}-{flink-shaded.version}.jar
* hive-exec-2.x.jar (对于 Hive 1.x 版本，你需要复制 hive-exec-1.x.jar, hive-metastore-1.x.jar, libfb303-0.9.2.jar and libthrift-0.9.2.jar)

然后你就可以在 Scala Shell 中使用 Hive 连接器，如下所示：

{% highlight scala %}
Scala-Flink> import org.apache.flink.table.catalog.hive.HiveCatalog
Scala-Flink> val hiveCatalog = new HiveCatalog("hive", "default", "<Replace it with HIVE_CONF_DIR>", "2.3.4");
Scala-Flink> btenv.registerCatalog("hive", hiveCatalog)
Scala-Flink> btenv.useCatalog("hive")
Scala-Flink> btenv.listTables
Scala-Flink> btenv.sqlQuery("<sql query>").toDataSet[Row].print()
{% endhighlight %}
