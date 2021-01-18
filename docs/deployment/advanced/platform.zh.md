---
title: "平台用户的可扩展功能"
nav-title: 平台
nav-parent_id: advanced
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

Flink通过插件框架为平台用户提供了一些自制定的功能.

## 定制错误监听器
Flink提高了一错误监听器接口。其中的的默认错误监听器会记录错误的次数，同时产生metric "numJobFailure"。用户可以定制多个错误监听器。当一个运行时错误发生时，
这些对象会被调用。这些监听器可以用来创建基于错误类型的metrics，调用外部系统，或者对错误进行分类。比如区分错误是来自Flink的运行时还是用户的逻辑。通过准确
的Metrics，你可以对平台状态有更好的了解，比如，由于网络导致的错误次数，平台的可靠性等。


### 为你所使用的错误监听器实现自定义插件

要为你所使用的错误监听器实现自定义插件，你需要：

  - 添加你自定义的 FailureListener，该 FailureListener 需要实现  `org.apache.flink.core.failurelistener.FailureListener` 接口。
  
  - 添加你自定义的 FailureListenerFactory，该 FailureListenerFactory 需要实现  `org.apache.flink.core.failurelistener.FailureListenerFactory` 接口。

  - 添加服务入口。创建 `META-INF/services/org.apache.flink.core.failurelistener.FailureListenerFactory` 文件，其中包含了你自定义FailureListenerFactory的的类名（更多细节请参看 [Java Service Loader](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html)）。

之后，将自定义的 `FailureListener`，`FailureListenerFactory`, `META-INF/services/` 和所有外部依赖打入 jar 包。在你的 Flink 发行版的 `plugins/` 文件夹中创建一个名为“failure-listener”的文件夹，将打好的 jar 包放入其中。
更多细节请查看 [Flink Plugin]({% link deployment/filesystems/plugins.zh.md %})。

你在插件里使用logger的时候，可能log4j不能正确的初始化。这是Flink插件是在不同的类加载器里加载的导致的。这个时候你需要在`flink-conf.yaml`里添加一些的配置：
- `plugin.classloader.parent-first-patterns.additional: org.slf4j`
