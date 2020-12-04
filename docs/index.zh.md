---
title: "Apache Flink 文档"
nav-pos: 0
nav-title: '<i class="fa fa-home title" aria-hidden="true"></i> Home'
nav-parent_id: root
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

<p style="margin: 30px 60px 0 60px;text-align: center" markdown="1">
Apache Flink 是一个在无界和有界数据流上进行状态计算的框架和分布式处理引擎。 Flink 已经可以在所有常见的集群环境中运行，并以 in-memory 的速度和任意的规模进行计算。
</p>

<div class="row">
<div class="col-sm-6" markdown="1">

### 试用 Flink

如果您有兴趣使用 Flink, 可以试试我们的教程:

* [Local Installation]({% link try-flink/local_installation.zh.md %})
* [DataStream API 进行欺诈检测]({% link try-flink/datastream_api.zh.md %})
* [Table API 构建实时报表]({% link try-flink/table_api.zh.md %})
* [PyFlink 教程]({% link dev/python/index.zh.md %})
* [Flink 游乐场]({% link try-flink/flink-operations-playground.zh.md %})

### 学习 Flink

* [操作培训]({% link learn-flink/index.zh.md %}) 包含了一系列的课程和练习，提供了对 Flink 的逐一介绍。

* [概念透析]({% link concepts/index.zh.md %}) 介绍了在浏览参考文档之前你需要了解的 Flink 知识。

### 获取 Flink 帮助

如果你遇到困难了, 可以在 [社区](https://flink.apache.org/zh/community.html)寻求帮助。值得一提的是，Apache Flink 的用户邮件列表一直是 Apache 项目里面最活跃的之一，也是一个快速获得帮助的好途径。

</div>
<div class="col-sm-6" markdown="1">

### 探索 Flink

参考文档包含了 Flink 所有内容。 你可以从以下几点开始学习:

<div class="row">
<div class="col-sm-6" markdown="1">

* [DataStream API]({% link dev/datastream_api.zh.md %})
* [Table API &amp; SQL]({% link dev/table/index.zh.md %})
* [状态方法]({% if site.is_stable %} {{ site.statefundocs_stable_baseurl }} {% else %} {{ site.statefundocs_baseurl }} {% endif %})

</div>
<div class="col-sm-6" markdown="1">

* [配置参数]({% link deployment/config.zh.md %})
* [Rest API]({% link ops/rest_api.zh.md %})
* [CLI]({% link deployment/cli.zh.md %})

</div>
</div>

### 部署 Flink

在线上环境运行你的 Flink 作业之前，请阅读 [生产环境注意事项检查清单]({% link ops/production_ready.zh.md %}). 各种部署环境一览，详见 [集群与部署]({% link deployment/resource-providers/index.zh.md %}). 

### 升级 Flink

release notes 包含了 Flink 版本之间的重大更新。请在你升级 Flink 之前仔细阅读相应的 release notes。

请阅读 release notes [Flink 1.11]({% link release-notes/flink-1.11.zh.md %}), [Flink 1.10]({% link release-notes/flink-1.10.zh.md %}), [Flink 1.9]({% link release-notes/flink-1.9.zh.md %}), [Flink 1.8]({% link release-notes/flink-1.8.zh.md %}), or [Flink 1.7]({% link release-notes/flink-1.7.zh.md %}).

</div>
</div>

<div style="margin: 40px 0 0 0; position: relative; top: 20px;">
<p>
本文档适用于 Apache Flink {{ site.version_title }} 版本。本页面最近更新于: {% build_time %}.
</p>
</div>
